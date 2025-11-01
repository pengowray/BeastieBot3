# See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

# This stage is used when running from VS in fast mode (Default for Debug configuration)
FROM mcr.microsoft.com/dotnet/runtime:9.0 AS base
USER $APP_UID
WORKDIR /app

# Ensure dataset directories exist inside the container so host volumes can be mounted cleanly
RUN mkdir -p /app/datasets/Catalogue_of_Life_2025-10-10_XR
RUN mkdir -p /app/datasets/IUCN_CVS_2025-2

# This stage is used to build the service project
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["BeastieBot3/BeastieBot3.csproj", "BeastieBot3/"]
RUN dotnet restore "./BeastieBot3/BeastieBot3.csproj"
COPY . .
WORKDIR "/src/BeastieBot3"
RUN dotnet build "./BeastieBot3.csproj" -c $BUILD_CONFIGURATION -o /app/build

# This stage is used to publish the service project to be copied to the final stage
FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./BeastieBot3.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

# This stage is used in production or when running from VS in regular mode (Default when not using the Debug configuration)
FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "BeastieBot3.dll"]