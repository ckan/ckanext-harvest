"""add cascade to harvest tables

Revision ID: 75d650dfd519
Revises: 3b4894672727
Create Date: 2023-11-02 17:13:39.995339

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = "75d650dfd519"
down_revision = "3b4894672727"
branch_labels = None
depends_on = None


def upgrade():
    _recreate_fk("CASCADE")


def downgrade():
    _recreate_fk(None)


def _recreate_fk(ondelete):
    op.drop_constraint("harvest_job_source_id_fkey", "harvest_job")
    op.create_foreign_key(
        "harvest_job_source_id_fkey",
        "harvest_job",
        "harvest_source",
        ["source_id"],
        ["id"],
        ondelete=ondelete,
    )

    op.drop_constraint("harvest_object_harvest_job_id_fkey", "harvest_object")
    op.create_foreign_key(
        "harvest_object_harvest_job_id_fkey",
        "harvest_object",
        "harvest_job",
        ["harvest_job_id"],
        ["id"],
        ondelete=ondelete,
    )

    op.drop_constraint("harvest_object_harvest_source_id_fkey", "harvest_object")
    op.create_foreign_key(
        "harvest_object_harvest_source_id_fkey",
        "harvest_object",
        "harvest_source",
        ["harvest_source_id"],
        ["id"],
        ondelete=ondelete,
    )

    op.drop_constraint("harvest_object_package_id_fkey", "harvest_object")
    op.create_foreign_key(
        "harvest_object_package_id_fkey",
        "harvest_object",
        "package",
        ["package_id"],
        ["id"],
        ondelete=ondelete,
        deferrable=True,
    )

    op.drop_constraint(
        "harvest_object_extra_harvest_object_id_fkey", "harvest_object_extra"
    )
    op.create_foreign_key(
        "harvest_object_extra_harvest_object_id_fkey",
        "harvest_object_extra",
        "harvest_object",
        ["harvest_object_id"],
        ["id"],
        ondelete=ondelete,
    )

    op.drop_constraint(
        "harvest_gather_error_harvest_job_id_fkey", "harvest_gather_error"
    )
    op.create_foreign_key(
        "harvest_gather_error_harvest_job_id_fkey",
        "harvest_gather_error",
        "harvest_job",
        ["harvest_job_id"],
        ["id"],
        ondelete=ondelete,
    )

    op.drop_constraint(
        "harvest_object_error_harvest_object_id_fkey", "harvest_object_error"
    )
    op.create_foreign_key(
        "harvest_object_error_harvest_object_id_fkey",
        "harvest_object_error",
        "harvest_object",
        ["harvest_object_id"],
        ["id"],
        ondelete=ondelete,
    )
