/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.trino.dispatcher;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.stats.Distribution;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.cost.CostCalculator;
import io.trino.cost.StatsCalculator;
import io.trino.execution.Input;
import io.trino.execution.LocationFactory;
import io.trino.execution.QueryIdGenerator;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryPreparer;
import io.trino.execution.QueryState;
import io.trino.execution.QueryStateMachine;
import io.trino.execution.QueryStats;
import io.trino.execution.StageId;
import io.trino.execution.StageInfo;
import io.trino.execution.StageState;
import io.trino.execution.StageStats;
import io.trino.execution.TableInfo;
import io.trino.execution.warnings.WarningCollector;
import io.trino.memory.LocalMemoryManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableProperties;
import io.trino.metadata.TableSchema;
import io.trino.operator.BlockedReason;
import io.trino.security.AccessControl;
import io.trino.server.DynamicFilterService;
import io.trino.server.HttpRequestSessionContextFactory;
import io.trino.server.ProtocolConfig;
import io.trino.server.SessionContext;
import io.trino.server.SessionSupplier;
import io.trino.server.security.InternalPrincipal;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.eventlistener.StageGcStatistics;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.security.Identity;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Analyzer;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.planner.InputExtractor;
import io.trino.sql.planner.LogicalPlanner;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.PlanFragmenter;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.PlanOptimizersFactory;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.transaction.TransactionManager;
import org.joda.time.DateTime;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.server.HttpRequestSessionContextFactory.AUTHENTICATED_IDENTITY;
import static io.trino.server.security.ResourceSecurity.AccessType.AUTHENTICATED_USER;
import static io.trino.spi.StandardErrorCode.STACK_OVERFLOW;
import static io.trino.sql.ParameterUtils.parameterExtractor;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.util.StatementUtils.getQueryType;
import static io.trino.util.StatementUtils.isTransactionControlStatement;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;

@Path("/v1/vis")
public class Visualizer
{
    // private final QueryStateMachine stateMachine;
    private final PlanOptimizersFactory planOptimizersFactory;
    private final PlannerContext plannerContext;
    private final TypeAnalyzer typeAnalyzer;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final AnalyzerFactory analyzerFactory;
    // private final WarningCollector warningCollector;
    private final QueryPreparer queryPreparer;
    private final HttpRequestSessionContextFactory sessionContextFactory;
    private final Optional<String> alternateHeaderName;
    private final SessionSupplier sessionSupplier;
    private final QueryIdGenerator queryIdGenerator;
    private final PlanFragmenter planFragmenter;
    private final LocationFactory locationFactory;
    private final Metadata metadata;
    private final AccessControl accessControl;
    private final TransactionManager transactionManager;
    private final DispatchExecutor dispatchExecutor;

    @Inject
    public Visualizer(PlanOptimizersFactory planOptimizersFactory,
                      // QueryStateMachine stateMachine,
                      PlannerContext plannerContext,
                      TypeAnalyzer typeAnalyzer,
                      StatsCalculator statsCalculator,
                      CostCalculator costCalculator,
                      AnalyzerFactory analyzerFactory,
                      // WarningCollector warningCollector,
                      QueryPreparer queryPreparer,
                      HttpRequestSessionContextFactory sessionContextFactory,
                      ProtocolConfig protocolConfig,
                      SessionSupplier sessionSupplier,
                      QueryIdGenerator queryIdGenerator,
                      PlanFragmenter planFragmenter,
                      LocationFactory locationFactory,
                      Metadata metadata,
                      AccessControl accessControl,
                      TransactionManager transactionManager,
                      DispatchExecutor dispatchExecutor)
    {
        this.planOptimizersFactory = requireNonNull(planOptimizersFactory, "planOptimizersFactory is null");
        // this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
        this.plannerContext = requireNonNull(plannerContext, "planner context is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.analyzerFactory = requireNonNull(analyzerFactory, "analyzerFactory is null");
        // this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.queryPreparer = requireNonNull(queryPreparer, "queryPreparer is null");
        this.sessionContextFactory = requireNonNull(sessionContextFactory);
        this.alternateHeaderName = requireNonNull(protocolConfig, "protocolConfig is null").getAlternateHeaderName();
        this.sessionSupplier = requireNonNull(sessionSupplier, "sessionSupplier is null");
        this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator is null");
        this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.transactionManager = requireNonNull(transactionManager, "trasactionManager is null");
        this.dispatchExecutor = requireNonNull(dispatchExecutor, "dispatchExecutor is null");
    }

    @ResourceSecurity(AUTHENTICATED_USER)
    @POST
    @Produces(APPLICATION_JSON)
    public Response visStatement(
            String statement,
            @Context HttpServletRequest servletRequest,
            @Context HttpHeaders httpHeaders,
            @Context UriInfo uriInfo)
    {
        Optional<String> remoteAddress = Optional.ofNullable(servletRequest.getRemoteAddr());
        Optional<Identity> identity = Optional.ofNullable((Identity) servletRequest.getAttribute(AUTHENTICATED_IDENTITY));
        if (identity.flatMap(Identity::getPrincipal).map(InternalPrincipal.class::isInstance).orElse(false)) {
            throw badRequest(FORBIDDEN, "Internal communication can not be used to start a query");
        }

        MultivaluedMap<String, String> headers = httpHeaders.getRequestHeaders();

        SessionContext sessionContext = sessionContextFactory.createSessionContext(headers, alternateHeaderName, remoteAddress, identity);

        // TransactionId transactionId = transactionManager.beginTransaction(true);
        QueryId queryId = queryIdGenerator.createNextQueryId();
        Session session = sessionSupplier.createSession(queryId, sessionContext);
        QueryPreparer.PreparedQuery preparedQuery = queryPreparer.prepareQuery(session, statement);

        QueryStateMachine stateMachine = QueryStateMachine.begin(
                // sessionContext.getTransactionId(),
                statement,
                preparedQuery.getPrepareSql(),
                session,
                locationFactory.createQueryLocation(session.getQueryId()),
                new ResourceGroupId("explain"),
                isTransactionControlStatement(preparedQuery.getStatement()),
                transactionManager,
                accessControl,
                dispatchExecutor.getExecutor(),
                metadata,
                WarningCollector.NOOP,
                getQueryType(preparedQuery.getStatement()));

        Analysis analysis = analyze(preparedQuery, stateMachine, WarningCollector.NOOP, analyzerFactory);

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        LogicalPlanner logicalPlanner = new LogicalPlanner(stateMachine.getSession(),
                this.planOptimizersFactory.get(),
                idAllocator,
                plannerContext,
                typeAnalyzer,
                statsCalculator,
                costCalculator,
                stateMachine.getWarningCollector());
        Plan plan = logicalPlanner.plan(analysis);
        // queryPlan.set(plan);

        // fragment the plan
        SubPlan fragmentedPlan = planFragmenter.createSubPlans(stateMachine.getSession(), plan, false, stateMachine.getWarningCollector());

        // extract inputs
        List<Input> inputs = new InputExtractor(plannerContext.getMetadata(), stateMachine.getSession()).extractInputs(fragmentedPlan);
        stateMachine.setInputs(inputs);

        stateMachine.setOutput(analysis.getTarget());

        StageInfo stageInfo = getStageInfo(fragmentedPlan, queryId, stateMachine.getSession());

        QueryInfo queryInfo = new QueryInfo(
                queryId,
                session.toSessionRepresentation(),
                QueryState.FINISHED,
                LocalMemoryManager.GENERAL_POOL,
                true,
                URI.create("1"),
                ImmutableList.of(),
                statement,
                preparedQuery.getPrepareSql(),
                new QueryStats(
                        DateTime.now(),
                        DateTime.now(),
                        DateTime.now(),
                        DateTime.now(),
                        new Duration(0, MILLISECONDS),
                        new Duration(0, MILLISECONDS),
                        new Duration(0, MILLISECONDS),
                        new Duration(0, MILLISECONDS),
                        new Duration(0, MILLISECONDS),
                        new Duration(0, MILLISECONDS),
                        new Duration(0, MILLISECONDS),
                        new Duration(0, MILLISECONDS),
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        DataSize.ofBytes(0),
                        DataSize.ofBytes(0),
                        DataSize.ofBytes(0),
                        DataSize.ofBytes(0),
                        DataSize.ofBytes(0),
                        DataSize.ofBytes(0),
                        DataSize.ofBytes(0),
                        DataSize.ofBytes(0),
                        DataSize.ofBytes(0),
                        false,
                        new Duration(0, MILLISECONDS),
                        new Duration(0, MILLISECONDS),
                        new Duration(0, MILLISECONDS),
                        false,
                        ImmutableSet.of(),
                        DataSize.ofBytes(0),
                        0,
                        new Duration(0, MILLISECONDS),
                        DataSize.ofBytes(0),
                        0,
                        DataSize.ofBytes(0),
                        0,
                        DataSize.ofBytes(0),
                        0,
                        DataSize.ofBytes(0),
                        0,
                        DataSize.ofBytes(0),
                        ImmutableList.of(),
                        DynamicFilterService.DynamicFiltersStats.EMPTY,
                        ImmutableList.of()),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                null,
                Optional.of(stageInfo),
                null,
                null,
                ImmutableList.of(),
                ImmutableSet.of(),
                Optional.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                true,
                Optional.empty(),
                Optional.empty());

        Response.ResponseBuilder builder = Response.ok(queryInfo);
        return builder.build();
    }

    private StageInfo subPlan2StageInfo(SubPlan subPlan, QueryId queryId, Session session)
    {
        StageId stageId = getStageId(queryId, subPlan.getFragment().getId());
        return new StageInfo(
                stageId,
                StageState.FINISHED,
                subPlan.getFragment(),
                false,
                subPlan.getFragment().getTypes(),
                EXPECTED,
                ImmutableList.of(),
                ImmutableList.of(),
                extractTableInfo(session, metadata, subPlan.getFragment()),
                null);
    }

    private static Map<PlanNodeId, TableInfo> extractTableInfo(Session session, Metadata metadata, PlanFragment fragment)
    {
        return searchFrom(fragment.getRoot())
                .where(TableScanNode.class::isInstance)
                .findAll()
                .stream()
                .map(TableScanNode.class::cast)
                .collect(toImmutableMap(PlanNode::getId, node -> getTableInfo(session, metadata, node)));
    }

    private static TableInfo getTableInfo(Session session, Metadata metadata, TableScanNode node)
    {
        TableSchema tableSchema = metadata.getTableSchema(session, node.getTable());
        TableProperties tableProperties = metadata.getTableProperties(session, node.getTable());
        return new TableInfo(tableSchema.getQualifiedName(), tableProperties.getPredicate());
    }

    private static StageId getStageId(QueryId queryId, PlanFragmentId fragmentId)
    {
        // TODO: refactor fragment id to be based on an integer
        return new StageId(queryId, Integer.parseInt(fragmentId.toString()));
    }

    void buildChildren(SubPlan root, Map<PlanFragmentId, StageInfo> children, QueryId queryId, Session session)
    {
//        List<StageInfo> stageInfos = new ArrayList<>();
//        for (SubPlan child: root.getChildren()) {
//            stageInfos.add(subPlan2StageInfo(child, queryId, session));
//        }
        children.putIfAbsent(root.getFragment().getId(), subPlan2StageInfo(root, queryId, session));
        for (SubPlan child : root.getChildren()) {
            buildChildren(child, children, queryId, session);
        }
    }

    public StageInfo getStageInfo(SubPlan subPlan, QueryId queryId, Session session)
    {
        Map<PlanFragmentId, StageInfo> children = new HashMap<>();
        buildChildren(subPlan, children, queryId, session);
//        Map<StageId, StageInfo> stageInfos = stages.values().stream()
//                .map(SqlStage::getStageInfo)
//                .collect(toImmutableMap(StageInfo::getStageId, identity()));

        return buildStageInfo(subPlan, children);
    }

    private StageInfo buildStageInfo(SubPlan plan, Map<PlanFragmentId, StageInfo> stageInfos)
    {
        StageInfo parent = stageInfos.get(plan.getFragment().getId());
        checkArgument(parent != null, "No stageInfo for %s", parent);
        List<StageInfo> childStages = plan.getChildren().stream()
                .map(childStageId -> buildStageInfo(childStageId, stageInfos))
                .collect(toImmutableList());
        if (childStages.isEmpty()) {
            return parent;
        }
        return new StageInfo(
                parent.getStageId(),
                parent.getState(),
                parent.getPlan(),
                parent.isCoordinatorOnly(),
                parent.getTypes(),
                parent.getStageStats(),
                parent.getTasks(),
                childStages,
                parent.getTables(),
                parent.getFailureCause());
    }

    private static Analysis analyze(
            QueryPreparer.PreparedQuery preparedQuery,
            QueryStateMachine stateMachine,
            WarningCollector warningCollector,
            AnalyzerFactory analyzerFactory)
    {
        stateMachine.beginAnalysis();
        requireNonNull(preparedQuery, "preparedQuery is null");
        Analyzer analyzer = analyzerFactory.createAnalyzer(
                stateMachine.getSession(),
                preparedQuery.getParameters(),
                parameterExtractor(preparedQuery.getStatement(), preparedQuery.getParameters()),
                warningCollector);
        Analysis analysis;
        try {
            analysis = analyzer.analyze(preparedQuery.getStatement());
        }
        catch (StackOverflowError e) {
            throw new TrinoException(STACK_OVERFLOW, "statement is too large (stack overflow during analysis)", e);
        }

        stateMachine.setUpdateType(analysis.getUpdateType());
        stateMachine.setReferencedTables(analysis.getReferencedTables());
        stateMachine.setRoutines(analysis.getRoutines());

        stateMachine.endAnalysis();

        return analysis;
    }

    private static WebApplicationException badRequest(Response.Status status, String message)
    {
        throw new WebApplicationException(
                Response.status(status)
                        .type(TEXT_PLAIN_TYPE)
                        .entity(message)
                        .build());
    }

    private static final StageStats EXPECTED = new StageStats(
            new DateTime(0),

            getTestDistribution(1),

            4,
            5,
            6,
            1,

            7,
            8,
            10,
            26,
            11,

            12.0,
            DataSize.ofBytes(14),
            DataSize.ofBytes(15),
            DataSize.ofBytes(16),
            DataSize.ofBytes(17),
            DataSize.ofBytes(18),
            new Duration(19, NANOSECONDS),
            new Duration(20, NANOSECONDS),
            new Duration(21, NANOSECONDS),
            false,
            new HashSet<BlockedReason>(),
            // new Duration(22, NANOSECONDS),
            // new Duration(23, NANOSECONDS),
            // ImmutableSet.of(),

            DataSize.ofBytes(191),
            0L,
            new Duration(22, NANOSECONDS),

            DataSize.ofBytes(192),
            201,

            DataSize.ofBytes(18),
            202,

            DataSize.ofBytes(18),
            99L,
            DataSize.ofBytes(18),
            DataSize.ofBytes(18),
            100L,
            DataSize.ofBytes(193),

            new StageGcStatistics(
                    101,
                    102,
                    103,
                    104,
                    105,
                    106,
                    107),

            ImmutableList.of());

    private static Distribution.DistributionSnapshot getTestDistribution(int count)
    {
        Distribution distribution = new Distribution();
        for (int i = 0; i < count; i++) {
            distribution.add(i);
        }
        return distribution.snapshot();
    }
}
