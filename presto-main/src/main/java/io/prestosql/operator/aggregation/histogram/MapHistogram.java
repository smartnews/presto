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
package io.prestosql.operator.aggregation.histogram;

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.DynamicClassLoader;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.SqlAggregationFunction;
import io.prestosql.operator.aggregation.AccumulatorCompiler;
import io.prestosql.operator.aggregation.AggregationMetadata;
import io.prestosql.operator.aggregation.GenericAccumulatorFactoryBinder;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignatureParameter;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.metadata.Signature.comparableTypeParameter;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.prestosql.operator.aggregation.AggregationUtils.generateAggregationName;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.util.Reflection.methodHandle;

public class MapHistogram
        extends SqlAggregationFunction
{
    public static final MapHistogram MAP_HISTOGRAM = new MapHistogram(HistogramGroupImplementation.NEW);
    public static final String NAME = "map_histogram";
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(MapHistogram.class, "output", Type.class, HistogramState.class, BlockBuilder.class);
    private static final MethodHandle INPUT_FUNCTION = methodHandle(MapHistogram.class, "input", Type.class, HistogramState.class, Block.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(MapHistogram.class, "combine", HistogramState.class, HistogramState.class);

    public static final int EXPECTED_SIZE_FOR_HASHING = 10;
    private final HistogramGroupImplementation groupMode;

    public MapHistogram(HistogramGroupImplementation groupMode)
    {
        super(NAME, ImmutableList.of(comparableTypeParameter("K")),
                ImmutableList.of(),
                parseTypeSignature("map<K,bigint>"),
                ImmutableList.of(parseTypeSignature("map<K,bigint>")));
        this.groupMode = groupMode;
    }

    @Override
    public String getDescription()
    {
        return "Sum the value for each key in maps";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = boundVariables.getTypeVariable("K");
        Type outputType = typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(keyType.getTypeSignature()),
                TypeSignatureParameter.of(BIGINT.getTypeSignature())));
        return generateAggregation(keyType, outputType, groupMode);
    }

    private static InternalAggregationFunction generateAggregation(Type keyType, Type outputType,
            HistogramGroupImplementation groupMode)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(MapHistogram.class.getClassLoader());
        List<Type> inputTypes = ImmutableList.of(outputType);
        HistogramStateSerializer stateSerializer = new HistogramStateSerializer(keyType, outputType);
        Type intermediateType = stateSerializer.getSerializedType();
        MethodHandle inputFunction = INPUT_FUNCTION.bindTo(keyType);
        MethodHandle outputFunction = OUTPUT_FUNCTION.bindTo(outputType);

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, outputType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(outputType),
                inputFunction,
                COMBINE_FUNCTION,
                outputFunction,
                ImmutableList.of(new AggregationMetadata.AccumulatorStateDescriptor(
                        HistogramState.class,
                        stateSerializer,
                        new HistogramStateFactory(keyType, EXPECTED_SIZE_FOR_HASHING, groupMode))),
                outputType);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, ImmutableList.of(intermediateType), outputType, true, false, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type inputType)
    {
        return ImmutableList.of(
                new ParameterMetadata(STATE),
                new ParameterMetadata(INPUT_CHANNEL, inputType));
    }

    public static void input(Type keyType, HistogramState state, Block value)
    {
        TypedHistogram typedHistogram = state.get();

        long startSize = typedHistogram.getEstimatedSize();
        for (int i = 0; i < value.getPositionCount(); i += 2) {
            typedHistogram.add(i, value, BIGINT.getLong(value, i + 1));
        }
        state.addMemoryUsage(typedHistogram.getEstimatedSize() - startSize);
    }

    public static void combine(HistogramState state, HistogramState otherState)
    {
        Histogram.combine(state, otherState);
    }

    public static void output(Type type, HistogramState state, BlockBuilder out)
    {
        Histogram.output(type, state, out);
    }
}
