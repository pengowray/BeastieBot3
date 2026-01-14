# AGENTS.md

## Project Overview

BeastieBot3 is a .NET 9.0 CLI application designed to aggregate and process biological taxonomy data from multiple sources including IUCN, Catalogue of Life (COL), Wikidata, and Wikipedia. It uses Spectre.Console for CLI interactions and SQLite for local caching and data storage.

## Build and Run

### Build
The project uses the standard .NET SDK build system.
- **Build**: `dotnet build`
- **Clean**: `dotnet clean`

### Run
The application is a CLI tool with a hierarchical command structure.
- **Run**: `dotnet run --project BeastieBot3/BeastieBot3.csproj -- [command] [options]`
- **Example**: `dotnet run --project BeastieBot3/BeastieBot3.csproj -- iucn import`

**Main Commands:**
- `col`: Catalogue of Life imports and checks.
- `iucn`: IUCN Red List data imports, reports, and API caching.
- `wikidata`: Wikidata seeding, caching, and cross-referencing with IUCN.
- `wikipedia`: Wikipedia page caching, matching, and list generation.

### Tests
There is currently no formal test suite (xUnit/NUnit) configured in the solution.
- **Verification**: Developers should verify changes by building the project (`dotnet build`) and running the relevant CLI command.
- **Future**: If unit tests are added, they should follow standard .NET conventions (`dotnet test`).

## Code Style & Conventions

### General
- **Language**: C# (.NET 9.0)
- **Formatting**:
  - Use 4 spaces for indentation.
  - Open braces `{` on the same line.
  - Use `var` when the type is obvious from the right-hand side.
  - Use file-scoped namespaces (e.g., `namespace BeastieBot3;`) where possible.
- **Nullability**: Nullable reference types are enabled (`<Nullable>enable</Nullable>`). Use `?` suffixes and handle null checks.

### Naming
- **Classes/Methods**: PascalCase (e.g., `ApiImportMetadataStore`, `EnsureSchema`).
- **Variables/Parameters**: camelCase (e.g., `importId`, `httpStatus`).
- **Private Fields**: `_camelCase` (e.g., `_connection`).
- **Constants**: PascalCase or ALL_CAPS depending on usage.

### Dependencies & Imports
- **Imports**: Group `using` statements at the top of the file.
- **Libraries**:
  - `Spectre.Console` for CLI interfaces.
  - `Microsoft.Data.Sqlite` for database interactions.
  - `Microsoft.Extensions.Configuration` for settings (INI files).

### Error Handling
- Use `try-catch` blocks for external operations.
- Throw standard exceptions (`ArgumentNullException`) for invalid arguments.
- Use `Spectre.Console` to display user-friendly error messages.

### Database (SQLite)
- Use raw SQL queries with `SqliteCommand` for performance.
- Always use parameterized queries (`@param`) to prevent SQL injection.
- Manage connections with `using` statements.

## Architecture

- **Commands**: Implemented using `Spectre.Console.Cli`. Commands are organized into branches (e.g., `iucn`, `wikidata`, `wikipedia`) in `Program.cs`.
- **Services/Repositories**: Encapsulate logic for specific domains (e.g., `IucnImporter`, `WikidataCacheStore`).
- **Data Access**: Direct SQLite interactions or helper classes like `ApiImportMetadataStore`.
- **Legacy Code**: The `BeastieBot3/BeastieLegacy/` directory contains older code (e.g., `LatinSpecies`, `DupeFinder`) that is not actively run but serves as a reference for logic and business rules.

## Agent Instructions

1. **Analysis**: Before editing, use `grep` and `glob` to find relevant files. Read surrounding code to match style.
2. **Implementation**:
   - Create new commands by inheriting from `Command<TSettings>` or `AsyncCommand<TSettings>`.
   - Register new commands in `Program.cs`.
   - Use `ApiImportMetadataStore` or similar patterns for tracking data operations.
3. **Verification**: Since there are no unit tests, verify your changes by:
   - Compiling: `dotnet build`
   - Running the command you modified/created: `dotnet run --project BeastieBot3/BeastieBot3.csproj -- [command]`
   - Checking output/logs for correctness.
