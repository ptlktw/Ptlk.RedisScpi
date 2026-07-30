FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base
WORKDIR /app
RUN mkdir -p /data

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY ["src/Ptlk.SCADA.Interop/Ptlk.SCADA.Interop/Ptlk.SCADA.Interop.csproj", "src/Ptlk.SCADA.Interop/Ptlk.SCADA.Interop/"]
COPY ["src/Ptlk.RedisScpi/Ptlk.RedisScpi.csproj", "src/Ptlk.RedisScpi/"]
RUN --mount=type=secret,id=ptlk_ca,required=false \
    if [ -s /run/secrets/ptlk_ca ]; then \
      cat /etc/ssl/certs/ca-certificates.crt /run/secrets/ptlk_ca > /tmp/ptlk-build-ca-bundle.crt \
      && SSL_CERT_FILE=/tmp/ptlk-build-ca-bundle.crt dotnet restore "src/Ptlk.RedisScpi/Ptlk.RedisScpi.csproj" \
      && rm -f /tmp/ptlk-build-ca-bundle.crt; \
    else dotnet restore "src/Ptlk.RedisScpi/Ptlk.RedisScpi.csproj"; fi
COPY ["src/Ptlk.SCADA.Interop/Ptlk.SCADA.Interop/", "src/Ptlk.SCADA.Interop/Ptlk.SCADA.Interop/"]
COPY ["src/Ptlk.RedisScpi/", "src/Ptlk.RedisScpi/"]
WORKDIR "/src/src/Ptlk.RedisScpi"
RUN dotnet publish "Ptlk.RedisScpi.csproj" -c Release -o /app/publish --no-restore /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=build /app/publish /app
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
VOLUME ["/data"]
ENTRYPOINT ["dotnet", "/app/Ptlk.RedisScpi.dll"]
