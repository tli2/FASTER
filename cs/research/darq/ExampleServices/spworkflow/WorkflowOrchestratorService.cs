using System.Text;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace dse.services;

public class WorkflowOrchestratorService : WorkflowOrchestrator.WorkflowOrchestratorBase
{
    private OrchestratorBackgroundProcessingService backend;
    private ILogger<WorkflowOrchestratorService> logger;
    public WorkflowOrchestratorService(OrchestratorBackgroundProcessingService backend, ILogger<WorkflowOrchestratorService> logger)
    {
        this.backend = backend;
        this.logger = logger;
    }
    
    public override Task<ExecuteWorkflowResult> ExecuteWorkflow(ExecuteWorkflowRequest request,
        ServerCallContext context)
    {
        // logger.LogInformation(
            // $"Received execute workflow request, id of {request.WorkflowId}, class id of {request.WorkflowClassId}, request string of {Encoding.UTF8.GetString(request.Input.Span)}");
        return backend.CreateWorkflow(request);
    }
}