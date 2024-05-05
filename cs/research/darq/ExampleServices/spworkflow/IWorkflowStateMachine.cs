using FASTER.libdpr;

namespace dse.services;


public interface IWorkflowStateMachine
{
    public void ProcessMessage(DarqMessage m);

    public void OnRestart(IDarqProcessorClientCapabilities capabilities, StateObject stateObject);

    Task<ExecuteWorkflowResult> GetResult(CancellationToken token);
}