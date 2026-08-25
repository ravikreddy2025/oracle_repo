from __future__ import annotations

import pytest

from ingestion_framework.orchestration.bundle import (
    BundleError,
    BundleOptions,
    build_job,
    build_resources,
    build_task,
    cross_group_dependency_report,
    email_targets,
    group_specs,
    job_name,
    resolve_group_schedule,
    task_key,
)

from .test_batch_runner import table


def job_for(specs, group="g", env="prod", **kwargs):
    return build_job(group, specs, env=env, options=BundleOptions(**kwargs))


class TestNaming:
    def test_task_key_is_identifier_safe(self):
        assert task_key("finance.gl_transactions") == "finance_gl_transactions"

    def test_job_name_carries_env_and_group(self):
        assert job_name("prod", "finance_hourly") == "ingest_prod_finance_hourly"


class TestGrouping:
    def test_tables_bucket_by_schedule_group(self):
        grouped = group_specs([
            table("a", group="hourly"), table("b", group="daily"), table("c", group="hourly")
        ])
        assert set(grouped) == {"hourly", "daily"}
        assert [s.table_fqn for s in grouped["hourly"]] == ["finance.a", "finance.c"]

    def test_order_within_a_group_is_stable(self):
        grouped = group_specs([table("c"), table("a"), table("b")])
        assert [s.table_fqn for s in grouped["default"]] == ["finance.a", "finance.b", "finance.c"]


class TestSchedule:
    def test_single_cron_is_used(self):
        specs = [table("a", schedule={"group": "g", "cron": "0 15 * * * ?"})]
        assert resolve_group_schedule("g", specs) == ("0 15 * * * ?", "UTC")

    def test_tables_without_cron_leave_the_job_unscheduled(self):
        assert resolve_group_schedule("g", [table("a")]) == (None, "UTC")

    def test_conflicting_crons_are_rejected_with_the_fix(self):
        # A group becomes one job, and a job has one schedule.
        specs = [
            table("a", schedule={"group": "g", "cron": "0 15 * * * ?"}),
            table("b", schedule={"group": "g", "cron": "0 30 * * * ?"}),
        ]
        with pytest.raises(BundleError, match="split the tables into separate groups"):
            resolve_group_schedule("g", specs)

    def test_conflicting_timezones_are_rejected(self):
        specs = [
            table("a", schedule={"group": "g", "timezone": "UTC"}),
            table("b", schedule={"group": "g", "timezone": "Asia/Kolkata"}),
        ]
        with pytest.raises(BundleError, match="conflicting timezones"):
            resolve_group_schedule("g", specs)

    def test_schedule_reaches_the_job(self):
        job = job_for([table("a", schedule={"group": "g", "cron": "0 15 * * * ?",
                                            "timezone": "Asia/Kolkata"})])
        assert job["schedule"] == {
            "quartz_cron_expression": "0 15 * * * ?",
            "timezone_id": "Asia/Kolkata",
            "pause_status": "UNPAUSED",
        }

    def test_unscheduled_job_has_no_schedule_block(self):
        assert "schedule" not in job_for([table("a")])


class TestTask:
    def task(self, spec=None, **kwargs):
        spec = spec or table("gl")
        return build_task(
            spec,
            options=BundleOptions(**kwargs),
            env_expression="${bundle.target}",
            known_tables={spec.table_fqn},
        )

    def test_invokes_the_cli_entry_point(self):
        task = self.task()
        wheel = task["python_wheel_task"]
        assert wheel["package_name"] == "ingestion_framework"
        assert wheel["entry_point"] == "ingest"
        assert wheel["parameters"][:3] == ["run", "--table", "finance.gl"]

    def test_env_comes_from_the_bundle_target(self):
        # One generated file per env, but the env still resolves at deploy time.
        assert "${bundle.target}" in self.task()["python_wheel_task"]["parameters"]

    def test_config_root_points_at_the_deployed_tree(self):
        assert "${workspace.file_path}/config" in self.task()["python_wheel_task"]["parameters"]

    def test_trigger_is_marked_as_scheduled(self):
        parameters = self.task()["python_wheel_task"]["parameters"]
        assert parameters[-2:] == ["--trigger", "schedule"]

    def test_timeout_comes_from_runtime_config(self):
        assert self.task(table("gl", runtime={"timeout_minutes": 240}))["timeout_seconds"] == 14400

    def test_job_retries_default_off(self):
        # The framework retries source errors itself and records each attempt;
        # job retries on top would multiply attempts and blur the audit trail.
        assert self.task()["max_retries"] == 0

    def test_job_retries_can_be_raised_for_infrastructure_failure(self):
        spec = table("gl", schedule={"group": "default", "job_retries": 2})
        assert self.task(spec)["max_retries"] == 2

    def test_dependency_becomes_a_task_edge(self):
        spec = table("child", depends_on=["finance.parent"])
        task = build_task(
            spec,
            options=BundleOptions(),
            env_expression="prod",
            known_tables={"finance.child", "finance.parent"},
        )
        assert task["depends_on"] == [{"task_key": "finance_parent"}]

    def test_dependency_outside_the_job_is_not_an_edge(self):
        spec = table("child", depends_on=["sales.elsewhere"])
        task = build_task(
            spec, options=BundleOptions(), env_expression="prod", known_tables={"finance.child"}
        )
        assert "depends_on" not in task


class TestJob:
    def test_one_task_per_table(self):
        job = job_for([table("a"), table("b")])
        assert [t["task_key"] for t in job["tasks"]] == ["finance_a", "finance_b"]

    def test_concurrent_runs_are_capped_at_one(self):
        # Two concurrent runs of a table would race each other's watermark.
        assert job_for([table("a")])["max_concurrent_runs"] == 1

    def test_job_cluster_is_defined_and_referenced(self):
        job = job_for([table("a")])
        assert job["job_clusters"][0]["job_cluster_key"] == "ingest_cluster"
        assert job["tasks"][0]["job_cluster_key"] == "ingest_cluster"

    def test_cluster_sizing_comes_from_bundle_variables(self):
        cluster = job_for([table("a")])["job_clusters"][0]["new_cluster"]
        assert cluster["node_type_id"] == "${var.node_type}"
        assert cluster["data_security_mode"] == "SINGLE_USER"  # required for UC

    def test_tags_mark_the_job_as_managed(self):
        tags = job_for([table("a", group="hourly")], group="hourly")["tags"]
        assert tags["managed_by"] == "ingestion-framework"
        assert tags["schedule_group"] == "hourly"

    def test_empty_group_is_refused(self):
        with pytest.raises(BundleError, match="has no tables"):
            build_job("g", [], env="prod")

    def test_cycle_is_rejected_before_a_job_is_emitted(self):
        specs = [
            table("a", depends_on=["finance.b"]),
            table("b", depends_on=["finance.a"]),
        ]
        with pytest.raises(Exception, match="cycle"):
            job_for(specs)

    def test_cross_group_dependency_is_noted_in_the_artefact(self):
        # Silently dropping an ordering constraint is the worst outcome.
        job = job_for([table("child", depends_on=["sales.elsewhere"])])
        assert "Cross-group dependencies" in job["description"]


class TestNotifications:
    def test_email_channels_become_job_notifications(self):
        job = job_for([table("a", alerting={"on_failure": ["email:oncall@example.com"]})])
        assert job["email_notifications"]["on_failure"] == ["oncall@example.com"]

    def test_non_email_channels_are_left_to_the_dispatcher(self):
        job = job_for([table("a", alerting={"on_failure": ["slack:#alerts"]})])
        assert "email_notifications" not in job

    def test_addresses_are_deduplicated_across_tables(self):
        job = job_for([
            table("a", alerting={"on_failure": ["email:x@y.com"]}),
            table("b", alerting={"on_failure": ["email:x@y.com", "email:z@y.com"]}),
        ])
        assert job["email_notifications"]["on_failure"] == ["x@y.com", "z@y.com"]

    def test_email_targets_parsing(self):
        assert email_targets(["email:a@b.com", "slack:#c", "webhook:https://x"]) == ["a@b.com"]


class TestResources:
    def test_one_job_per_group(self):
        resources = build_resources(
            [table("a", group="hourly"), table("b", group="daily")], env="prod"
        )
        assert set(resources["resources"]["jobs"]) == {"ingest_hourly", "ingest_daily"}

    def test_disabled_tables_are_excluded(self):
        resources = build_resources(
            [table("a", group="g"), table("b", group="g", runtime={"enabled": False})],
            env="prod",
        )
        tasks = resources["resources"]["jobs"]["ingest_g"]["tasks"]
        assert [t["task_key"] for t in tasks] == ["finance_a"]

    def test_a_group_of_only_disabled_tables_produces_no_job(self):
        resources = build_resources(
            [table("a", group="g", runtime={"enabled": False})], env="prod"
        )
        assert resources["resources"]["jobs"] == {}

    def test_cross_group_report_names_both_groups(self):
        report = cross_group_dependency_report([
            table("child", group="hourly", depends_on=["finance.parent"]),
            table("parent", group="daily"),
        ])
        assert report == [("finance.child", "hourly", "finance.parent", "daily")]

    def test_same_group_dependency_is_not_reported(self):
        report = cross_group_dependency_report([
            table("child", group="g", depends_on=["finance.parent"]),
            table("parent", group="g"),
        ])
        assert report == []


class TestShippedConfig:
    def test_generates_a_job_per_shipped_group(self, shipped_config):
        from ingestion_framework.config import build_run_spec
        from ingestion_framework.config.resolver import ConfigResolver

        specs = [
            build_run_spec(shipped_config, fqn, "prod")
            for fqn in ConfigResolver(shipped_config).list_tables()
        ]
        jobs = build_resources(specs, env="prod")["resources"]["jobs"]
        assert set(jobs) == {"ingest_finance_hourly", "ingest_finance_daily", "ingest_sales_hourly"}
        hourly = jobs["ingest_finance_hourly"]
        assert hourly["schedule"]["quartz_cron_expression"] == "0 15 * * * ?"
        assert hourly["email_notifications"]["on_failure"] == ["data-eng-oncall@example.com"]
