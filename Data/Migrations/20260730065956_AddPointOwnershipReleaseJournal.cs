using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Ptlk.RedisScpi.Data.Migrations
{
    /// <inheritdoc />
    public partial class AddPointOwnershipReleaseJournal : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "point_ownership_release_intents",
                columns: table => new
                {
                    id = table.Column<int>(type: "INTEGER", nullable: false)
                        .Annotation("Sqlite:Autoincrement", true),
                    operation_id = table.Column<string>(type: "TEXT", maxLength: 32, nullable: false),
                    converter_id = table.Column<string>(type: "TEXT", maxLength: 160, nullable: false),
                    source_path = table.Column<string>(type: "TEXT", maxLength: 320, nullable: true),
                    redis_key = table.Column<string>(type: "TEXT", maxLength: 320, nullable: false),
                    reason = table.Column<string>(type: "TEXT", maxLength: 40, nullable: false),
                    completion_action = table.Column<string>(type: "TEXT", maxLength: 32, nullable: false),
                    replacement_source_path = table.Column<string>(type: "TEXT", maxLength: 320, nullable: true),
                    replacement_redis_key = table.Column<string>(type: "TEXT", maxLength: 320, nullable: true),
                    status = table.Column<string>(type: "TEXT", maxLength: 32, nullable: false),
                    attempt_count = table.Column<int>(type: "INTEGER", nullable: false),
                    next_attempt_at = table.Column<long>(type: "INTEGER", nullable: false),
                    last_result_code = table.Column<string>(type: "TEXT", maxLength: 80, nullable: true),
                    last_error_message = table.Column<string>(type: "TEXT", maxLength: 1000, nullable: true),
                    requested_at = table.Column<long>(type: "INTEGER", nullable: false),
                    updated_at = table.Column<long>(type: "INTEGER", nullable: false),
                    redis_released_at = table.Column<long>(type: "INTEGER", nullable: true),
                    applied_at = table.Column<long>(type: "INTEGER", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("pk_point_ownership_release_intents", x => x.id);
                });

            migrationBuilder.CreateIndex(
                name: "ix_point_ownership_release_intents_operation_id",
                table: "point_ownership_release_intents",
                column: "operation_id",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "ix_point_ownership_release_intents_redis_key",
                table: "point_ownership_release_intents",
                column: "redis_key",
                unique: true,
                filter: "\"status\" <> 'applied'");

            migrationBuilder.CreateIndex(
                name: "ix_point_ownership_release_intents_replacement_redis_key",
                table: "point_ownership_release_intents",
                column: "replacement_redis_key",
                unique: true,
                filter: "\"replacement_redis_key\" IS NOT NULL AND \"status\" <> 'applied'");

            migrationBuilder.CreateIndex(
                name: "ix_point_ownership_release_intents_source_path_status",
                table: "point_ownership_release_intents",
                columns: new[] { "source_path", "status" });

            migrationBuilder.CreateIndex(
                name: "ix_point_ownership_release_intents_status_next_attempt_at_requested_at",
                table: "point_ownership_release_intents",
                columns: new[] { "status", "next_attempt_at", "requested_at" });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "point_ownership_release_intents");
        }
    }
}
