"""Prepare reviewed release files or publish an already reviewed main commit."""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import shutil
import subprocess
import tempfile
import tomllib
from contextlib import contextmanager
from pathlib import Path

RELEASE_FILES = ("pyproject.toml", "src/mcp_gateway/__init__.py", "CHANGELOG.md")


def command(*args: str) -> str:
    return subprocess.run(args, check=True, text=True, capture_output=True).stdout.strip()


def next_version() -> str:
    version = command("semantic-release", "version", "--print")
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        raise ValueError("Semantic Release did not return a stable version")
    return version


def committed_version() -> str:
    version = tomllib.loads(Path("pyproject.toml").read_text())["project"]["version"]
    assignments = ast.parse(Path("src/mcp_gateway/__init__.py").read_text()).body
    package_versions = [
        ast.literal_eval(node.value)
        for node in assignments
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "__version__" for target in node.targets)
    ]
    if not re.fullmatch(r"\d+\.\d+\.\d+", version) or package_versions != [version]:
        raise ValueError("Committed package versions must match and be stable")
    return version


def release_notes(version: str) -> str:
    changelog = Path("CHANGELOG.md").read_text()
    section = re.search(rf"(?m)^## v{re.escape(version)}(?:\s[^\n]*)?\n", changelog)
    if section is None:
        raise ValueError("The committed changelog has no section for this version")
    following = re.search(r"(?m)^## ", changelog[section.end():])
    end = section.end() + following.start() if following else len(changelog)
    notes = changelog[section.end():end].strip()
    if not notes:
        raise ValueError("Release notes must not be empty")
    return notes


def output(**values: str) -> None:
    if path := os.getenv("GITHUB_OUTPUT"):
        with Path(path).open("a") as handle:
            for key, value in values.items():
                handle.write(f"{key}={value}\n")
    print(json.dumps(values))


@contextmanager
def candidate_checkout():
    """Include main's release tags without merging main back into develop."""
    source = Path.cwd()
    source_sha = command("git", "rev-parse", "HEAD")
    main_sha = command("git", "rev-parse", "origin/main")
    remote = command("git", "remote", "get-url", "origin")
    with tempfile.TemporaryDirectory(prefix="release-candidate-") as directory:
        candidate = Path(directory) / "repo"
        command("git", "clone", "--quiet", "--shared", "--no-checkout", str(source), str(candidate))
        os.chdir(candidate)
        try:
            command("git", "fetch", "--quiet", "--tags", str(source))
            command("git", "remote", "set-url", "origin", remote)
            command("git", "checkout", "-B", "main", main_sha)
            command(
                "git", "-c", "user.name=Release Preparation", "-c", "user.email=release@example.invalid",
                "merge", "--no-ff", "-m", "Merge develop for release preparation", source_sha,
            )
            yield candidate
        finally:
            os.chdir(source)


def prepare() -> None:
    source = Path.cwd()
    date = command("git", "show", "-s", "--format=%cs", "HEAD")
    with candidate_checkout() as candidate:
        version = next_version()
        # A published version with no new release-driving commits needs no new PR.
        if command("git", "tag", "--list", f"v{version}"):
            output(version=version, changed="false")
            return
        previous_changelog = source / "CHANGELOG.md"
        if previous_changelog.exists():
            previous_date = re.search(
                rf"(?m)^## v{re.escape(version)} \((\d{{4}}-\d{{2}}-\d{{2}})\)$",
                previous_changelog.read_text(),
            )
            if previous_date:
                date = previous_date[1]
        command(
            "semantic-release", "version", "--no-commit", "--no-tag", "--no-push",
            "--no-vcs-release", "--skip-build",
        )
        if committed_version() != version:
            raise ValueError("Prepared package versions do not match the calculated release")
        changelog = Path("CHANGELOG.md")
        # Stable contents allow the same preparation PR to be retried on another day.
        text = re.sub(
            rf"(?m)^(## v{re.escape(version)}) \(\d{{4}}-\d{{2}}-\d{{2}}\)$",
            lambda match: f"{match[1]} ({date})", changelog.read_text(),
        )
        changelog.write_text(text)
        release_notes(version)
        for file in RELEASE_FILES:
            if candidate / file != source / file:
                shutil.copyfile(candidate / file, source / file)
    changes = command("git", "status", "--porcelain", "--", *RELEASE_FILES)
    output(version=version, changed="true" if changes else "false")


def api(path: str, *args: str, missing_ok: bool = False) -> dict | None:
    result = subprocess.run(["gh", "api", path, *args], text=True, capture_output=True)
    if result.returncode:
        if missing_ok and "(HTTP 404)" in result.stderr:
            return None
        raise RuntimeError(f"GitHub API request failed: {path}")
    return json.loads(result.stdout)


def tag_commit(repository: str, tag: str) -> str | None:
    ref = api(f"repos/{repository}/git/ref/tags/{tag}", missing_ok=True)
    if ref is None:
        return None
    obj = ref["object"]
    while obj["type"] == "tag":
        obj = api(f"repos/{repository}/git/tags/{obj['sha']}")["object"]
    if obj["type"] != "commit":
        raise ValueError("Release tag must point to a commit")
    return obj["sha"]


def publish(repository: str, expected_sha: str) -> None:
    if command("git", "rev-parse", "HEAD") != expected_sha:
        raise ValueError("Checkout does not match the release event commit")
    version = committed_version()
    notes = release_notes(version)
    tag = f"v{version}"
    tagged_sha = tag_commit(repository, tag)
    if tagged_sha is not None and tagged_sha != expected_sha:
        # Later non-release promotions retain the current version and tag.
        ancestor = subprocess.run(
            ["git", "merge-base", "--is-ancestor", tagged_sha, expected_sha], check=False,
        ).returncode == 0
        if ancestor and next_version() == version:
            if api(f"repos/{repository}/releases/tags/{tag}", missing_ok=True) is None:
                raise ValueError("Previous release is incomplete; rerun publication at its tagged commit")
            output(version=version, published="false")
            return
        raise ValueError("Existing release tag does not match the prepared commit")
    if tagged_sha is None:
        if next_version() != version:
            raise ValueError("Release files are not prepared for this commit; run release preparation first")
        api(
            f"repos/{repository}/git/refs", "--method", "POST",
            "-f", f"ref=refs/tags/{tag}", "-f", f"sha={expected_sha}",
        )
    # Recover safely if tag creation succeeded but release creation failed.
    existing = api(f"repos/{repository}/releases/tags/{tag}", missing_ok=True)
    if existing is None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".md") as handle:
            handle.write(notes)
            handle.flush()
            command(
                "gh", "release", "create", tag, "--repo", repository, "--verify-tag",
                "--title", tag, "--notes-file", handle.name,
            )
    if tag_commit(repository, tag) != expected_sha:
        raise ValueError("Published tag does not match the release event commit")
    output(version=version, published="true")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "publish"))
    parser.add_argument("--repository")
    parser.add_argument("--sha")
    args = parser.parse_args()
    if args.mode == "prepare":
        prepare()
    elif not args.repository or not args.sha:
        parser.error("publish requires --repository and --sha")
    else:
        publish(args.repository, args.sha)


if __name__ == "__main__":
    main()
