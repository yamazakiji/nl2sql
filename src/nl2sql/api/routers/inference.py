from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from nl2sql.api.dependencies.database import get_session
from nl2sql.api.schemas import (
    ChatMessage,
    ChatRequest,
    ChatResponse,
    ExecuteRequest,
    ExecuteResponse,
    InferenceRunList,
    InferenceRunSummary,
    PlanCandidate,
    PlanRequest,
    PlanResponse,
)
from nl2sql.observability import log_manager
from nl2sql.service.connectors import get_connector
from nl2sql.service.deployments import get_deployment
from nl2sql.service.inference import (
    execute_inference,
    get_inference_run,
    list_inference_runs,
    plan_inference,
)

router = APIRouter()


@router.post("/inference/plan", response_model=PlanResponse)
async def plan_endpoint(
    payload: PlanRequest, session: AsyncSession = Depends(get_session)
) -> PlanResponse:
    provider_override: str = "openai"
    deployment = await get_deployment(session, payload.deployment)
    connector = await get_connector(session, payload.connector)

    try:
        run, candidates, clarifications = await plan_inference(
            session,
            deployment=deployment,
            connector=connector,
            question=payload.question,
            provider=provider_override,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    await session.commit()

    await log_manager.emit(run.id, "plan ready for approval")

    return PlanResponse(
        run_id=run.id,
        candidates=[PlanCandidate(sql=candidates, rationale=clarifications, explain_summary="", est_cost=0.0)],
        clarifications=["",],
    )


@router.post("/inference/execute", response_model=ExecuteResponse)
async def execute_endpoint(
    payload: ExecuteRequest, session: AsyncSession = Depends(get_session)
) -> ExecuteResponse:
    run = await get_inference_run(session, payload.run_id)
    connector = await get_connector(session, payload.connector)

    if run.connector_id != connector.id:
        raise HTTPException(status_code=400, detail="Connector mismatch for run")

    try:
        run, rows = await execute_inference(
            session,
            run=run,
            connector=connector,
            approved_sql=payload.approved_sql,
            limit=payload.limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    await session.commit()

    return ExecuteResponse(run_id=run.id, row_count=len(rows), rows=rows, result_ref=run.result_path or "")


@router.post("/inference/chat", response_model=ChatResponse)
async def chat_endpoint(
    payload: ChatRequest, session: AsyncSession = Depends(get_session)
) -> ChatResponse:
    if not payload.history:
        raise HTTPException(status_code=400, detail="Provide at least one user message.")

    last_message = payload.history[-1]
    if last_message.role != "user":
        raise HTTPException(status_code=400, detail="Last message must come from the user.")

    provider_override: str | None = None
    if payload.deployment.lower() == "openai":
        deployment = None
        provider_override = "openai"
    else:
        deployment = await get_deployment(session, payload.deployment)
    connector = await get_connector(session, payload.connector)

    run, sql, explanation = await plan_inference(
        session,
        deployment=deployment,
        connector=connector,
        question=last_message.content,
        provider=provider_override,
    )
    await session.commit()

    top_candidate = sql
    assistant_reply = (
        "Proposed SQL:\n"
        f"{top_candidate}\n"
        f"Run ID: {run.id}. Approve and execute if this looks good."
    )

    messages = list(payload.history)
    messages.append(ChatMessage(role="assistant", content=assistant_reply))
    return ChatResponse(run_id=run.id, messages=messages)


@router.get("/inference/runs", response_model=InferenceRunList)
async def list_runs(limit: int = 20, session: AsyncSession = Depends(get_session)) -> InferenceRunList:
    runs = await list_inference_runs(session, limit)
    return InferenceRunList(items=[InferenceRunSummary.model_validate(run) for run in runs])
