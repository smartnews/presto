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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.ExceededMemoryLimitException;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.state.HistogramState;
import com.facebook.presto.operator.aggregation.state.HistogramStateFactory;
import com.facebook.presto.operator.aggregation.state.HistogramStateSerializer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

public class MapHistogram
        extends SqlAggregationFunction
{
    public static final MapHistogram MAP_HISTOGRAM = new MapHistogram();
    public static final String NAME = "map_histogram";
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(MapHistogram.class, "output", Type.class, HistogramState.class, BlockBuilder.class);
    private static final MethodHandle INPUT_FUNCTION = methodHandle(MapHistogram.class, "input", Type.class, HistogramState.class, Block.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(MapHistogram.class, "combine", HistogramState.class, HistogramState.class);

    public static final int EXPECTED_SIZE_FOR_HASHING = 10;

    public MapHistogram()
    {
        super(NAME, ImmutableList.of(comparableTypeParameter("K")),
                ImmutableList.of(),
                parseTypeSignature("map<K,bigint>"),
                ImmutableList.of(parseTypeSignature("map<K,bigint>")));
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
        return generateAggregation(keyType, outputType);
    }

    private static InternalAggregationFunction generateAggregation(Type keyType, Type outputType)
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
                HistogramState.class,
                stateSerializer,
                new HistogramStateFactory(),
                outputType);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, intermediateType, outputType, true, factory);
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
        if (typedHistogram == null) {
            typedHistogram = new TypedHistogram(keyType, EXPECTED_SIZE_FOR_HASHING);
            state.set(typedHistogram);
        }

        long startSize = typedHistogram.getEstimatedSize();
        try {
            for (int i = 0; i < value.getPositionCount(); i += 2) {
                typedHistogram.add(i, value, BIGINT.getLong(value, i + 1));
            }
        }
        catch (ExceededMemoryLimitException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("The result of histogram may not exceed %s", e.getMaxMemory()));
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
