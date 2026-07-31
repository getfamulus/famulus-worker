"""The stage prompt is what actually instructs the agent.

It is assembled from task text that may have come from an imported ticket, so
these tests pin the structure the pipeline depends on: every step_id present,
the result-file contract stated, and later stages not re-fetching context.
"""

from famulus_worker import worker


def steps(*names, task_title="Add rate limiting", description="", notes=None):
    notes = notes or {}
    return [
        {
            "step_id": f"id-{n}",
            "step_name": n,
            "step_note": notes.get(n, ""),
            "task_title": task_title,
            "task_description": description,
        }
        for n in names
    ]


class TestFirstStage:
    def test_includes_the_task_title_and_description(self):
        p = worker._build_stage_prompt(
            steps("Implement", description="Bursts saturate the pool"),
            "/w/.tr/t1", first_stage=True, local_paths=[],
        )
        assert "TASK: Add rate limiting" in p
        assert "DESCRIPTION: Bursts saturate the pool" in p

    def test_omits_the_description_line_when_empty(self):
        p = worker._build_stage_prompt(steps("Implement"), "/d", True, [])
        assert "DESCRIPTION:" not in p

    def test_tells_the_agent_to_work_autonomously(self):
        p = worker._build_stage_prompt(steps("Implement"), "/d", True, [])
        assert "autonomously" in p

    def test_lists_attached_files_when_present(self):
        p = worker._build_stage_prompt(
            steps("Implement"), "/d", True, ["/cache/a/spec.pdf", "/cache/a/logo.png"],
        )
        assert "Attached files" in p
        assert "/cache/a/spec.pdf" in p
        assert "/cache/a/logo.png" in p

    def test_omits_the_attachments_section_when_there_are_none(self):
        p = worker._build_stage_prompt(steps("Implement"), "/d", True, [])
        assert "Attached files" not in p

    def test_states_the_job_count(self):
        p = worker._build_stage_prompt(steps("A", "B", "C"), "/d", True, [])
        assert "3 independent job(s)" in p


class TestLaterStage:
    def test_tells_the_agent_not_to_refetch_context(self):
        p = worker._build_stage_prompt(steps("Review"), "/d", first_stage=False, local_paths=[])
        assert "do not" in p.lower() and "re-fetch" in p
        assert "TASK: " not in p

    def test_still_states_the_job_count(self):
        p = worker._build_stage_prompt(steps("A", "B"), "/d", False, [])
        assert "2 job(s)" in p


class TestJobsAndResultContract:
    def test_every_step_id_appears_exactly_once(self):
        s = steps("Implement", "Review", "Ship")
        p = worker._build_stage_prompt(s, "/d", True, [])
        for step in s:
            assert p.count(f"step_id={step['step_id']}") == 1

    def test_jobs_are_numbered_in_order(self):
        p = worker._build_stage_prompt(steps("First", "Second"), "/d", True, [])
        assert p.index("1. [step_id=id-First]") < p.index("2. [step_id=id-Second]")

    def test_a_note_is_appended_to_its_job(self):
        p = worker._build_stage_prompt(
            steps("Implement", notes={"Implement": "use a token bucket"}), "/d", True, [],
        )
        assert "Implement — use a token bucket" in p

    def test_a_job_without_a_note_has_no_dash(self):
        p = worker._build_stage_prompt(steps("Implement"), "/d", True, [])
        assert "1. [step_id=id-Implement] Implement" in p
        assert "Implement —" not in p

    def test_states_where_to_write_results(self):
        p = worker._build_stage_prompt(steps("Implement"), "/work/.tr/t1", True, [])
        assert "/work/.tr/t1/<step_id>.json" in p

    def test_states_the_result_json_shape(self):
        p = worker._build_stage_prompt(steps("Implement"), "/d", True, [])
        assert '"status"' in p and "passed" in p and "failed" in p
        assert '"output"' in p

    def test_demands_a_result_file_for_every_job(self):
        p = worker._build_stage_prompt(steps("A", "B"), "/d", True, [])
        assert "EVERY job" in p


def test_task_text_is_never_shell_interpolated():
    """Task text reaches tmux via a paste buffer, so it is data, not a command.

    This pins the prompt builder's part of that: it does no escaping or
    quoting of its own, and passes the text through verbatim.
    """
    hostile = 'oops"; rm -rf ~; echo "'
    p = worker._build_stage_prompt(
        steps("Implement", task_title=hostile), "/d", True, [],
    )
    assert f"TASK: {hostile}" in p
