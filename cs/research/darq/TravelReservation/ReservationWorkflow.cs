using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using dse.services;
using FASTER.common;
using FASTER.core;
using FASTER.libdpr;
using FASTER.libdpr.gRPC;
using Google.Protobuf;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using protobuf;
using DarqMessage = FASTER.libdpr.DarqMessage;
using DarqMessageType = FASTER.libdpr.DarqMessageType;

namespace TravelReservation;

internal enum ReservationWorkflowMessageTypes : byte
{
    RESERVATION_START,
    RESERVATION_ROLLBACK
}

internal struct ActivityDarqEntry : ILogEnqueueEntry
{
    internal long workflowId;
    internal ReservationWorkflowMessageTypes type;
    internal int index;

    public int SerializedLength => sizeof(long) + sizeof(ReservationWorkflowMessageTypes) + sizeof(int);

    public void SerializeTo(Span<byte> dest)
    {
        unsafe
        {
            fixed (byte* d = dest)
            {
                var head = d;
                *(long*)head = workflowId;
                head += sizeof(long);
                *(ReservationWorkflowMessageTypes*)head = type;
                head += sizeof(ReservationWorkflowMessageTypes);
                *(int*)head = index;
            }
        }
    }
}

public class ReservationWorkflowStateMachine : IWorkflowStateMachine
{
    private long workflowId;
    private List<ReservationRequest> toExecute = new();
    private TaskCompletionSource<bool> tcs = new();
    private IDarqProcessorClientCapabilities capabilities;
    private SimpleObjectPool<StepRequest> stepRequestPool = new(() => new StepRequest());
    private ConcurrentDictionary<int, GrpcChannel> connectionPool;
    private IEnvironment environment;
    private bool speculative;
    private ILogger logger;

    public ReservationWorkflowStateMachine(ReadOnlySpan<byte> input,
        ConcurrentDictionary<int, GrpcChannel> connectionPool, IEnvironment environment, bool speculative, ILogger logger)
    {
        var messageString = Encoding.UTF8.GetString(input);
        var split = messageString.Split(',');
        workflowId = long.Parse(split[1]);
        for (var i = 2; i < split.Length; i += 4)
        {
            toExecute.Add(new ReservationRequest
            {
                ReservationId = long.Parse(split[i]),
                OfferingId = long.Parse(split[i + 1]),
                CustomerId = long.Parse(split[i + 2]),
                Count = int.Parse(split[i + 3])
            });
        }

        this.connectionPool = connectionPool;
        this.environment = environment;
        this.speculative = speculative;
        this.logger = logger;
    }

    public async Task<ExecuteWorkflowResult> GetResult(CancellationToken token)
    {
        var result = await tcs.Task.WaitAsync(token);
        return new ExecuteWorkflowResult
        {
            Ok = result,
            Result = ByteString.Empty,
        };
    }

    public void ProcessMessage(DarqMessage m)
    {
        if (m.GetMessageBody().Length == sizeof(long))
        {
            // Then this is the initial message, bootstrap the state machine and begin execution
            var stepRequest = stepRequestPool.Checkout();
            var requestBuilder = new StepRequestBuilder(stepRequest);

            requestBuilder.AddSelfMessage(new ActivityDarqEntry
            {
                workflowId = workflowId,
                type = ReservationWorkflowMessageTypes.RESERVATION_START,
                index = 0
            });
            requestBuilder.MarkMessageConsumed(m.GetLsn());
            m.Dispose();

            // Will always be completed synchronously
            Task.Run(async () =>
            {
                await capabilities.Step(requestBuilder.FinishStep());
                stepRequestPool.Return(stepRequest);
            });
            return;
        }

        Debug.Assert(m.GetMessageType() == DarqMessageType.IN);
        var lsn = m.GetLsn();
        var type = (ReservationWorkflowMessageTypes) m.GetMessageBody()[sizeof(long)];
        var index = BitConverter.ToInt32(
            m.GetMessageBody()[(sizeof(long) + sizeof(ReservationWorkflowMessageTypes))..]);
        if (type == ReservationWorkflowMessageTypes.RESERVATION_START)
            MakeReservation(lsn, index);
        else
            CancelReservation(lsn, index);
        m.Dispose();
    }

    private void MakeReservation(long lsn, int index)
    {
        if (index == toExecute.Count)
        {
            // logger.LogInformation($"Workflow with id {workflowId} completed successfully");
            // We are done and there are no more reservations to make
            tcs.SetResult(true);
            return;
        }

        var c = capabilities;
        Task.Run(async () =>
        {
            var channel = connectionPool.GetOrAdd(index,
                i => GrpcChannel.ForAddress(environment.GetServiceConnString(i)));
            var client = speculative
                ? new FasterKVReservationService.FasterKVReservationServiceClient(
                    channel.Intercept(new DprClientInterceptor(c.GetDprSession())))
                : new FasterKVReservationService.FasterKVReservationServiceClient(channel);

            // logger.LogInformation($"Workflow with id {workflowId} is starting reservation number {index}");
            var result = await client.MakeReservationAsync(toExecute[index]);
            // logger.LogInformation($"Workflow with id {workflowId} has completed reservation number {index}");
            var stepRequest = stepRequestPool.Checkout();
            var requestBuilder = new StepRequestBuilder(stepRequest);
            requestBuilder.MarkMessageConsumed(lsn);
            requestBuilder.AddSelfMessage(new ActivityDarqEntry
            {
                workflowId = workflowId,
                type = result.Ok
                    ? ReservationWorkflowMessageTypes.RESERVATION_START
                    : ReservationWorkflowMessageTypes.RESERVATION_ROLLBACK,
                index = result.Ok ? index + 1 : index - 1
            });
            // Will always be completed synchronously
            await c.Step(requestBuilder.FinishStep());
            stepRequestPool.Return(stepRequest);
        });
    }

    private void CancelReservation(long lsn, int index)
    {
        if (index == -1)
        {
            // logger.LogInformation($"Workflow with id {workflowId} completed with rollback");
            // We are done and there are no more reservations to make
            tcs.SetResult(false);
            return;
        }
        var c = capabilities;
        Task.Run(async () =>
        {
            var channel = connectionPool.GetOrAdd(index,
                k => GrpcChannel.ForAddress(environment.GetServiceConnString(index)));
            var client = speculative
                ? new FasterKVReservationService.FasterKVReservationServiceClient(
                    channel.Intercept(new DprClientInterceptor(c.GetDprSession())))
                : new FasterKVReservationService.FasterKVReservationServiceClient(channel);

            // logger.LogInformation($"Workflow with id {workflowId} is cancelling reservation number {index}");
            await client.CancelReservationAsync(toExecute[index]);
            // logger.LogInformation($"Workflow with id {workflowId} has cancelled reservation number {index}");
            var stepRequest = stepRequestPool.Checkout();
            var requestBuilder = new StepRequestBuilder(stepRequest);
            requestBuilder.MarkMessageConsumed(lsn);
            requestBuilder.AddSelfMessage(new ActivityDarqEntry
            {
                workflowId = workflowId,
                type = ReservationWorkflowMessageTypes.RESERVATION_ROLLBACK,
                index = index - 1
            });
            // Will always be completed synchronously
            await c.Step(requestBuilder.FinishStep());
            stepRequestPool.Return(stepRequest);
        });
    }

    public void OnRestart(IDarqProcessorClientCapabilities capabilities, StateObject backend)
    {
        this.capabilities = capabilities;
        // TODO(Tianyu): currently not actually a restart -- will only be called once at start and can only handle that
    }
}