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
package io.prestosql.operator.aggregation.smartnews.arbitraryn;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.bytecode.DynamicClassLoader;
import io.prestosql.metadata.FunctionArgumentDefinition;
import io.prestosql.metadata.FunctionBinding;
import io.prestosql.metadata.FunctionMetadata;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlAggregationFunction;
import io.prestosql.operator.aggregation.AccumulatorCompiler;
import io.prestosql.operator.aggregation.AggregationMetadata;
import io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata;
import io.prestosql.operator.aggregation.GenericAccumulatorFactoryBinder;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AccumulatorStateFactory;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.metadata.FunctionKind.AGGREGATE;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.prestosql.operator.aggregation.AggregationUtils.generateAggregationName;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.analyzer.TypeSignatureTranslator.parseTypeSignature;
import static io.prestosql.util.Reflection.methodHandle;
import static java.lang.Math.toIntExact;

/*
pretty similar to AbstractMinMaxNAggregationFunction
 */
public class ArbitraryNAggregationFunction
        extends SqlAggregationFunction
{
    public static final ArbitraryNAggregationFunction SMARTNEWS_ARBITRARY_N_AGGREGATION = new ArbitraryNAggregationFunction();

    private static final String NAME = "arbitrary_n";

    private static final MethodHandle INPUT_FUNCTION = methodHandle(ArbitraryNAggregationFunction.class, "input", Type.class, ArbitraryNState.class, Block.class, int.class, long.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(ArbitraryNAggregationFunction.class, "combine", ArbitraryNState.class, ArbitraryNState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(ArbitraryNAggregationFunction.class, "output", ArbitraryNState.class, BlockBuilder.class);

    protected ArbitraryNAggregationFunction()
    {
        super(new FunctionMetadata(
                new Signature(
                        NAME,
                        ImmutableList.of(typeVariable("T")),
                        ImmutableList.of(),
                        parseTypeSignature("array(T)", ImmutableSet.of()),
                        ImmutableList.of(parseTypeSignature("T", ImmutableSet.of()), parseTypeSignature(StandardTypes.BIGINT, ImmutableSet.of())),
                        false),
                    true,
                    ImmutableList.of(new FunctionArgumentDefinition(false)),
                    false,
                    true,
                    "return N arbitrary non-null input value",
                    AGGREGATE),
                true,
                false);
    }

    public static void input(Type type, ArbitraryNState state,
            Block value, int blockIndex, long n)
    {
        if (state.getData() == null) {
            if (n <= 0) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "second argument of arbitrary_n must be positive");
            }
            ArbitraryN data = new ArbitraryN(type, toIntExact(n));
            state.setData(data);
        }

        if (state.getData().isFull()) {
            return;
        }

        state.getData().add(value, blockIndex);
    }

    @CombineFunction
    public static void combine(ArbitraryNState state, ArbitraryNState otherState)
    {
        if (state.getData() == null) {
            if (otherState.getData() != null) {
                state.setData(otherState.getData());
                return;
            }
        }
        else {
            if (state.getData().isFull()) {
                return;
            }

            ArbitraryN otherData = otherState.getData();

            if (!otherData.isEmpty()) {
                int size = otherData.getPositionCount();
                for (int i = 0; i < size; i++) {
                    state.getData().add(otherState.getData().getBlockBuilder(), i);
                }
            }
        }
    }

    public static void output(ArbitraryNState state, BlockBuilder out)
    {
        if (state.getData().isEmpty()) {
            out.appendNull();
        }
        else {
            state.getData().writeAsArray(out);
        }
    }

    @Override
    public InternalAggregationFunction specialize(FunctionBinding functionBinding)
    {
        Type valueType = functionBinding.getTypeVariable("T");
        return generateAggregation(valueType);
    }

    private static InternalAggregationFunction generateAggregation(Type type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(ArbitraryNAggregationFunction.class.getClassLoader());

        AccumulatorStateFactory<?> stateFactory = new ArbitraryNStateFactory(type);

        List<Type> inputTypes = ImmutableList.of(type);
        AccumulatorStateSerializer<?> stateSerializer = new ArbitraryNStateSerializer(type);
        Type intermediateType = stateSerializer.getSerializedType();
        Type outputType = new ArrayType(type);

        List<ParameterMetadata> inputParameterMetadata = ImmutableList.of(
                new ParameterMetadata(STATE),
                new ParameterMetadata(BLOCK_INPUT_CHANNEL, type),
                new ParameterMetadata(BLOCK_INDEX),
                new ParameterMetadata(INPUT_CHANNEL, BIGINT));

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, type.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                inputParameterMetadata,
                INPUT_FUNCTION.bindTo(type),
                Optional.empty(),
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                ImmutableList.of(new AggregationMetadata.AccumulatorStateDescriptor(
                        ArbitraryNState.class,
                        stateSerializer,
                        stateFactory)),
                outputType);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, ImmutableList.of(intermediateType), outputType, factory);
    }
}
