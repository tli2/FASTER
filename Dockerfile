FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build-env
WORKDIR /app

# Build Travel Reservation
COPY ./cs .
WORKDIR /app/research/darq/TravelReservation
RUN dotnet restore
RUN dotnet build -c Release -o out

# Build EventProcessing
WORKDIR /app/research/darq/EventProcessing
RUN dotnet restore
RUN dotnet publish -c Release -o out

# Build CoordinatorMicrobench
WORKDIR /app/research/darq/CoordinatorMicrobench
RUN dotnet restore
RUN dotnet publish -c Release -o out

# Build TwoPhaseCommit
WORKDIR /app/research/darq/TwoPhaseCommit
RUN dotnet restore
RUN dotnet publish -c Release -o out

FROM mcr.microsoft.com/dotnet/aspnet:7.0
WORKDIR /app
COPY --from=build-env /app/TravelReservation-latency ./TravelReservation-latency
COPY --from=build-env /app/TravelReservation-thr ./TravelReservation-thr
COPY --from=build-env /app/EventProcessing-latency ./EventProcessing-latency
COPY --from=build-env /app/EventProcessing-recovery ./EventProcessing-recovery
COPY --from=build-env /app/research/darq/TravelReservation/out ./TravelReservation
COPY --from=build-env /app/research/darq/EventProcessing/out ./EventProcessing
COPY --from=build-env /app/research/darq/CoordinatorMicrobench/out ./CoordinatorMicrobench
COPY --from=build-env /app/research/darq/TwoPhaseCommit/out ./TwoPhaseCommit
EXPOSE 4022