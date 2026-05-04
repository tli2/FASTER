FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build-env
WORKDIR /app
COPY ./cs .

# Build all experiment drivers and microbenches
WORKDIR /app/research/darq/TravelReservation
RUN dotnet restore && dotnet publish -c Release -o out

WORKDIR /app/research/darq/TravelReservationTemporal
RUN dotnet restore && dotnet publish -c Release -o out

WORKDIR /app/research/darq/EventProcessing
RUN dotnet restore && dotnet publish -c Release -o out

WORKDIR /app/research/darq/TwoPhaseCommit
RUN dotnet restore && dotnet publish -c Release -o out

WORKDIR /app/research/darq/TwoPhaseCommitOrleans
RUN dotnet restore && dotnet publish -c Release -o out

WORKDIR /app/research/darq/CoordinatorMicrobench
RUN dotnet restore && dotnet publish -c Release -o out

WORKDIR /app/research/darq/StateObjectMicrobench
RUN dotnet restore && dotnet publish -c Release -o out

WORKDIR /app/research/darq/SpFasterMicrobench
RUN dotnet restore && dotnet publish -c Release -o out

FROM mcr.microsoft.com/dotnet/aspnet:9.0
RUN apt-get update && apt-get install -y libaio1 && rm -rf /var/lib/apt/lists/*
WORKDIR /app

# Workload bundles. Helm charts in cs/research/darq/*/helm-workload reference
# these as /app/<experiment>/workloads/<file> via the chart's `workload` value.
COPY ./AE/workloads/TravelReservation-latency  ./TravelReservation-latency
COPY ./AE/workloads/TravelReservation-thr      ./TravelReservation-thr
COPY ./AE/workloads/EventProcessing-latency    ./EventProcessing-latency
COPY ./AE/workloads/EventProcessing-recovery   ./EventProcessing-recovery

# Compiled binaries
COPY --from=build-env /app/research/darq/TravelReservation/out          ./TravelReservation
COPY --from=build-env /app/research/darq/TravelReservationTemporal/out  ./TravelReservationTemporal
COPY --from=build-env /app/research/darq/EventProcessing/out            ./EventProcessing
COPY --from=build-env /app/research/darq/TwoPhaseCommit/out             ./TwoPhaseCommit
COPY --from=build-env /app/research/darq/TwoPhaseCommitOrleans/out      ./TwoPhaseCommitOrleans
COPY --from=build-env /app/research/darq/CoordinatorMicrobench/out      ./CoordinatorMicrobench
COPY --from=build-env /app/research/darq/StateObjectMicrobench/out      ./StateObjectMicrobench
COPY --from=build-env /app/research/darq/SpFasterMicrobench/out         ./SpFasterMicrobench

EXPOSE 4022
