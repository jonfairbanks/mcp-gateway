from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path

import pytest

WORKFLOWS = Path(__file__).resolve().parents[1] / '.github' / 'workflows'
REPOSITORY = 'jonfairbanks/mcp-gateway'
TRUSTED = {'head': {'repo': {'full_name': REPOSITORY}, 'sha': 'same-sha'}, 'html_url': 'https://example.test/trusted'}
FORK = {'head': {'repo': {'full_name': 'someone/mcp-gateway'}, 'sha': 'same-sha'}, 'html_url': 'https://example.test/fork'}


@pytest.mark.parametrize('filename,base,head', [
    ('prepare-release.yml', 'develop', '$branch'),
    ('weekly-develop-to-main.yml', 'main', 'develop'),
])
@pytest.mark.parametrize('pages,expected', [
    ([[FORK, TRUSTED]], TRUSTED['html_url']),
    ([[FORK], [TRUSTED]], TRUSTED['html_url']),
    ([[FORK]], ''),
    ([[{'head': {'repo': None}, 'html_url': 'https://example.test/deleted'}]], ''),
    ([[{'html_url': 'https://example.test/missing'}]], ''),
    ([[{'head': {'repo': {'full_name': 'jonfairbanks/other'}}, 'html_url': 'https://example.test/other'}]], ''),
    ([[TRUSTED]], TRUSTED['html_url']),
    ([[]], ''),
])
def test_release_workflow_selects_only_trusted_repository(filename, base, head, pages, expected):
    workflow = (WORKFLOWS / filename).read_text()
    selection = re.search(r'pr_url="\$\((gh api .*?)\)"', workflow, re.DOTALL)
    assert selection is not None
    command = selection[1]
    assert '--method GET --paginate' in command
    assert f'-f base={base}' in command
    assert f'-f "head=${{GH_REPO%%/*}}:{head}"' in command
    query = re.search(r"jq -rs '([^']+)'", command)
    assert query is not None
    result = subprocess.run(
        ['jq', '-rs', query[1]],
        input='\n'.join(json.dumps(page) for page in pages),
        env={**os.environ, 'GH_REPO': REPOSITORY},
        text=True,
        capture_output=True,
        check=True,
    )
    assert result.stdout.strip() == expected
