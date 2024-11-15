"""create harvest tables

Revision ID: 3b4894672727
Revises:
Create Date: 2023-11-02 15:53:02.262586

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "3b4894672727"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    engine = op.get_bind()
    inspector = sa.inspect(engine)
    tables = inspector.get_table_names()
    if "harvest_source" not in tables:
        op.create_table(
            "harvest_source",
            sa.Column("id", sa.UnicodeText, primary_key=True),
            sa.Column("url", sa.UnicodeText, nullable=False),
            sa.Column("title", sa.UnicodeText),
            sa.Column("description", sa.UnicodeText),
            sa.Column("config", sa.UnicodeText),
            sa.Column("created", sa.DateTime),
            sa.Column("type", sa.UnicodeText, nullable=False),
            sa.Column("active", sa.Boolean),
            sa.Column("user_id", sa.UnicodeText),
            sa.Column("publisher_id", sa.UnicodeText),
            sa.Column("frequency", sa.UnicodeText),
            sa.Column("next_run", sa.DateTime),
        )

    if "harvest_job" not in tables:
        op.create_table(
            "harvest_job",
            sa.Column("id", sa.UnicodeText, primary_key=True),
            sa.Column("created", sa.DateTime),
            sa.Column("gather_started", sa.DateTime),
            sa.Column("gather_finished", sa.DateTime),
            sa.Column("finished", sa.DateTime),
            sa.Column(
                "source_id",
                sa.UnicodeText,
                sa.ForeignKey("harvest_source.id"),
            ),
            sa.Column("status", sa.UnicodeText, nullable=False),
        )

    if "harvest_object" not in tables:
        op.create_table(
            "harvest_object",
            sa.Column("id", sa.UnicodeText, primary_key=True),
            sa.Column("guid", sa.UnicodeText),
            sa.Column("current", sa.Boolean),
            sa.Column("gathered", sa.DateTime),
            sa.Column("fetch_started", sa.DateTime),
            sa.Column("content", sa.UnicodeText, nullable=True),
            sa.Column("fetch_finished", sa.DateTime),
            sa.Column("import_started", sa.DateTime),
            sa.Column("import_finished", sa.DateTime),
            sa.Column("state", sa.UnicodeText),
            sa.Column("metadata_modified_date", sa.DateTime),
            sa.Column("retry_times", sa.Integer),
            sa.Column(
                "harvest_job_id",
                sa.UnicodeText,
                sa.ForeignKey("harvest_job.id"),
            ),
            sa.Column(
                "harvest_source_id",
                sa.UnicodeText,
                sa.ForeignKey("harvest_source.id"),
            ),
            sa.Column(
                "package_id",
                sa.UnicodeText,
                sa.ForeignKey("package.id", deferrable=True),
                nullable=True,
            ),
            sa.Column("report_status", sa.UnicodeText, nullable=True),
        )

    index_names = [index["name"] for index in inspector.get_indexes("harvest_object")]
    if "harvest_job_id_idx" not in index_names:
        op.create_index("harvest_job_id_idx", "harvest_object", ["harvest_job_id"])

    if "harvest_source_id_idx" not in index_names:
        op.create_index(
            "harvest_source_id_idx", "harvest_object", ["harvest_source_id"]
        )

    if "package_id_idx" not in index_names:
        op.create_index("package_id_idx", "harvest_object", ["package_id"])

    if "guid_idx" not in index_names:
        op.create_index("guid_idx", "harvest_object", ["guid"])

    if "harvest_object_extra" not in tables:
        op.create_table(
            "harvest_object_extra",
            sa.Column("id", sa.UnicodeText, primary_key=True),
            sa.Column(
                "harvest_object_id",
                sa.UnicodeText,
                sa.ForeignKey("harvest_object.id"),
            ),
            sa.Column("key", sa.UnicodeText),
            sa.Column("value", sa.UnicodeText),
        )

    index_names = [
        index["name"] for index in inspector.get_indexes("harvest_object_extra")
    ]
    if "harvest_object_id_idx" not in index_names:
        op.create_index(
            "harvest_object_id_idx", "harvest_object_extra", ["harvest_object_id"]
        )

    if "harvest_gather_error" not in tables:
        op.create_table(
            "harvest_gather_error",
            sa.Column("id", sa.UnicodeText, primary_key=True),
            sa.Column(
                "harvest_job_id",
                sa.UnicodeText,
                sa.ForeignKey("harvest_job.id"),
            ),
            sa.Column("message", sa.UnicodeText),
            sa.Column("created", sa.DateTime),
        )

    if "harvest_object_error" not in tables:
        op.create_table(
            "harvest_object_error",
            sa.Column("id", sa.UnicodeText, primary_key=True),
            sa.Column(
                "harvest_object_id",
                sa.UnicodeText,
                sa.ForeignKey("harvest_object.id"),
            ),
            sa.Column("message", sa.UnicodeText),
            sa.Column("stage", sa.UnicodeText),
            sa.Column("line", sa.Integer),
            sa.Column("created", sa.DateTime),
        )

    index_names = [
        index["name"] for index in inspector.get_indexes("harvest_object_error")
    ]
    if "harvest_error_harvest_object_id_idx" not in index_names:
        op.create_index(
            "harvest_error_harvest_object_id_idx",
            "harvest_object_error",
            ["harvest_object_id"],
        )

    if "harvest_log" not in tables:
        op.create_table(
            "harvest_log",
            sa.Column("id", sa.UnicodeText, primary_key=True),
            sa.Column("content", sa.UnicodeText, nullable=False),
            sa.Column(
                "level",
                sa.Enum(
                    "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", name="log_level"
                ),
            ),
            sa.Column("created", sa.DateTime),
        )


def downgrade():
    op.drop_table("harvest_log")
    sa.Enum(name="log_level").drop(op.get_bind())
    op.drop_table("harvest_object_error")
    op.drop_table("harvest_gather_error")
    op.drop_table("harvest_object_extra")
    op.drop_table("harvest_object")
    op.drop_table("harvest_job")
    op.drop_table("harvest_source")
