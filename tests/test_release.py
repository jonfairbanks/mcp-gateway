from __future__ import annotations

import subprocess
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts import release


@pytest.fixture
def prepared_repo(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "src/mcp_gateway").mkdir(parents=True)
    (tmp_path / "pyproject.toml").write_text('[project]\nversion = "1.2.0"\n')
    (tmp_path / "src/mcp_gateway/__init__.py").write_text('__version__ = "1.2.0"\n')
    (tmp_path / "CHANGELOG.md").write_text("# Changelog\n\n## v1.2.0 (2026-10-03)\n\nNew tools.\n\n## v1.1.0\nOld tools.\n")
    @contextmanager
    def candidate():
        yield tmp_path

    monkeypatch.setattr(release, "candidate_checkout", candidate)
    return tmp_path


def test_release_versions_and_notes_are_checked(prepared_repo):
    assert release.committed_version() == "1.2.0"
    assert release.release_notes("1.2.0") == "New tools."
    (prepared_repo / "src/mcp_gateway/__init__.py").write_text('__version__ = "1.1.0"\n')
    with pytest.raises(ValueError, match="versions must match"):
        release.committed_version()
    with pytest.raises(ValueError, match="no section"):
        release.release_notes("2.0.0")


@pytest.mark.parametrize("text", ["", "warning\n1.2.0", "1.2.0-rc.1"])
def test_next_version_rejects_unexpected_output(monkeypatch, text):
    monkeypatch.setattr(release, "command", lambda *args: text)
    with pytest.raises(ValueError, match="stable version"):
        release.next_version()


def test_preparation_never_commits_tags_or_pushes(prepared_repo, monkeypatch):
    calls = []
    results = []

    def command(*args):
        calls.append(args)
        if args == ("semantic-release", "version", "--print"):
            return "1.2.0"
        if args[:3] == ("git", "show", "-s"):
            return "2026-10-03"
        if args[:3] == ("git", "status", "--porcelain"):
            return " M CHANGELOG.md"
        return ""

    monkeypatch.setattr(release, "command", command)
    monkeypatch.setattr(release, "output", lambda **values: results.append(values))
    release.prepare()
    assert results == [{"version": "1.2.0", "changed": "true"}]
    generation = next(call for call in calls if "--no-commit" in call)
    assert {"--no-tag", "--no-push", "--no-vcs-release", "--skip-build"} <= set(generation)
    assert all(call[:2] not in [("git", "push"), ("git", "commit")] for call in calls)


def test_preparation_skips_an_already_published_version(prepared_repo, monkeypatch):
    monkeypatch.setattr(release, "next_version", lambda: "1.2.0")
    monkeypatch.setattr(release, "command", lambda *args: "v1.2.0")
    results = []
    monkeypatch.setattr(release, "output", lambda **values: results.append(values))
    release.prepare()
    assert results == [{"version": "1.2.0", "changed": "false"}]


@pytest.mark.parametrize("tag_exists,release_exists", [(False, False), (True, False), (True, True)])
def test_publication_creates_only_tag_and_release_and_recovers_retries(
    prepared_repo, monkeypatch, tag_exists, release_exists,
):
    calls = []
    tag_reads = iter(["merge-sha" if tag_exists else None, "merge-sha"])
    monkeypatch.setattr(release, "tag_commit", lambda *args: next(tag_reads))
    monkeypatch.setattr(release, "next_version", lambda: "1.2.0")

    def command(*args):
        calls.append(args)
        return "merge-sha" if args[:2] == ("git", "rev-parse") else ""

    def api(path, *args, **kwargs):
        calls.append((path, *args))
        return {"id": 1} if release_exists else None

    monkeypatch.setattr(release, "command", command)
    monkeypatch.setattr(release, "api", api)
    release.publish("owner/repo", "merge-sha")
    tag_writes = [call for call in calls if "POST" in call]
    assert len(tag_writes) == (0 if tag_exists else 1)
    if tag_writes:
        assert "ref=refs/tags/v1.2.0" in tag_writes[0]
        assert "sha=merge-sha" in tag_writes[0]
    creates = [call for call in calls if call[:3] == ("gh", "release", "create")]
    assert len(creates) == (0 if release_exists else 1)
    assert all(call[:2] not in [("git", "push"), ("git", "commit")] for call in calls)


def test_publication_rejects_unprepared_version_before_remote_writes(prepared_repo, monkeypatch):
    monkeypatch.setattr(release, "command", lambda *args: "merge-sha")
    monkeypatch.setattr(release, "tag_commit", lambda *args: None)
    monkeypatch.setattr(release, "next_version", lambda: "2.0.0")
    monkeypatch.setattr(release, "api", lambda *args, **kwargs: pytest.fail("Unexpected remote mutation"))
    with pytest.raises(ValueError, match="not prepared"):
        release.publish("owner/repo", "merge-sha")


def test_publication_rejects_wrong_checkout(prepared_repo, monkeypatch):
    monkeypatch.setattr(release, "command", lambda *args: "different-sha")
    with pytest.raises(ValueError, match="event commit"):
        release.publish("owner/repo", "merge-sha")


@pytest.mark.parametrize("ancestor,calculated", [(False, "1.2.0"), (True, "1.3.0")])
def test_publication_rejects_conflicting_tags(prepared_repo, monkeypatch, ancestor, calculated):
    monkeypatch.setattr(release, "command", lambda *args: "merge-sha")
    monkeypatch.setattr(release, "tag_commit", lambda *args: "other-sha")
    monkeypatch.setattr(release, "next_version", lambda: calculated)
    monkeypatch.setattr(release.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=0 if ancestor else 1))
    with pytest.raises(ValueError, match="does not match"):
        release.publish("owner/repo", "merge-sha")


def test_non_release_promotion_does_not_move_existing_tag(prepared_repo, monkeypatch):
    monkeypatch.setattr(release, "command", lambda *args: "merge-sha")
    monkeypatch.setattr(release, "tag_commit", lambda *args: "released-sha")
    monkeypatch.setattr(release, "next_version", lambda: "1.2.0")
    monkeypatch.setattr(release.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=0))
    monkeypatch.setattr(release, "api", lambda *args, **kwargs: {"id": 1})
    results = []
    monkeypatch.setattr(release, "output", lambda **values: results.append(values))
    release.publish("owner/repo", "merge-sha")
    assert results == [{"version": "1.2.0", "published": "false"}]


def test_tag_commit_resolves_annotated_tags(monkeypatch):
    responses = iter([
        {"object": {"type": "tag", "sha": "tag-sha"}},
        {"object": {"type": "commit", "sha": "commit-sha"}},
    ])
    monkeypatch.setattr(release, "api", lambda *args, **kwargs: next(responses))
    assert release.tag_commit("owner/repo", "v1.2.0") == "commit-sha"


@pytest.mark.parametrize("stderr", ["authentication failed (HTTP 401)", "connection failed"])
def test_api_errors_are_not_treated_as_missing_tags(monkeypatch, stderr):
    monkeypatch.setattr(release.subprocess, "run", lambda *args, **kwargs: subprocess.CompletedProcess([], 1, "", stderr))
    with pytest.raises(RuntimeError, match="API request failed"):
        release.api("repos/owner/repo/git/ref/tags/v1.2.0", missing_ok=True)


def test_output_is_written_to_workflow_file(tmp_path, monkeypatch):
    path = tmp_path / "outputs"
    monkeypatch.setenv("GITHUB_OUTPUT", str(path))
    release.output(version="1.2.0", changed="true")
    assert path.read_text() == "version=1.2.0\nchanged=true\n"


def test_preparation_includes_main_only_tags_without_modifying_branch_history(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GIT_AUTHOR_DATE", "2026-10-03T12:00:00Z")
    monkeypatch.setenv("GIT_COMMITTER_DATE", "2026-10-03T12:00:00Z")
    git = release.command
    git("git", "init", "-b", "develop")
    git("git", "config", "user.name", "Test")
    git("git", "config", "user.email", "test@example.invalid")
    git("git", "remote", "add", "origin", "https://github.com/owner/repo.git")
    Path("src/mcp_gateway").mkdir(parents=True)
    Path("pyproject.toml").write_text('[project]\nversion = "1.0.0"\n')
    Path("src/mcp_gateway/__init__.py").write_text('__version__ = "1.0.0"\n')
    Path("CHANGELOG.md").write_text("## v1.0.0 (2026-10-01)\n\nInitial tools.\n")
    git("git", "add", ".")
    git("git", "commit", "-m", "feat: initial tools")
    git("git", "checkout", "-b", "main")
    git("git", "commit", "--allow-empty", "-m", "Merge develop for release")
    git("git", "tag", "v1.0.0")
    git("git", "update-ref", "refs/remotes/origin/main", git("git", "rev-parse", "HEAD"))
    git("git", "checkout", "develop")
    Path("tool.txt").write_text("new tool")
    git("git", "add", ".")
    git("git", "commit", "-m", "feat: add another tool")
    source_sha = git("git", "rev-parse", "HEAD")
    assert subprocess.run(["git", "merge-base", "--is-ancestor", "v1.0.0", "HEAD"]).returncode == 1

    def command(*args):
        if args[:2] != ("semantic-release", "version"):
            return git(*args)
        # Version calculation must see the main-only baseline and new develop feature.
        assert subprocess.run(["git", "merge-base", "--is-ancestor", "v1.0.0", "HEAD"]).returncode == 0
        assert subprocess.run(["git", "merge-base", "--is-ancestor", source_sha, "HEAD"]).returncode == 0
        if "--print" not in args:
            Path("pyproject.toml").write_text('[project]\nversion = "1.1.0"\n')
            Path("src/mcp_gateway/__init__.py").write_text('__version__ = "1.1.0"\n')
            Path("CHANGELOG.md").write_text("## v1.1.0 (2026-10-20)\n\nAnother tool.\n")
            return ""
        return "1.1.0"

    monkeypatch.setattr(release, "command", command)
    release.prepare()
    assert release.committed_version() == "1.1.0"
    assert "2026-10-03" in Path("CHANGELOG.md").read_text()
    assert git("git", "rev-parse", "HEAD") == source_sha
    assert git("git", "branch", "--show-current") == "develop"
    assert git("git", "tag", "--list") == "v1.0.0"
    monkeypatch.setenv("GIT_AUTHOR_DATE", "2026-10-04T12:00:00Z")
    monkeypatch.setenv("GIT_COMMITTER_DATE", "2026-10-04T12:00:00Z")
    git("git", "add", ".")
    git("git", "commit", "-m", "chore(release): 1.1.0")
    results = []
    monkeypatch.setattr(release, "output", lambda **values: results.append(values))
    release.prepare()
    assert results == [{"version": "1.1.0", "changed": "false"}]
    assert "2026-10-03" in Path("CHANGELOG.md").read_text()
