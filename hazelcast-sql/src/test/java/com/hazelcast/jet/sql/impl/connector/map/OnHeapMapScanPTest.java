/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.expression.ConstantPredicateExpression;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.plan.node.MapScanPlanNode;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;

import static com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext.SQL_ARGUMENTS_KEY_NAME;
import static com.hazelcast.sql.impl.SqlTestSupport.valuePath;
import static java.util.Collections.emptyList;

@SuppressWarnings("rawtypes")
@Category({QuickTest.class, ParallelJVMTest.class})
public class OnHeapMapScanPTest extends SimpleTestInClusterSupport {

    private static AtomicInteger id;
    private NodeEngine nodeEngine;
    private IMap<Integer, String> map;
    private IMap<Integer, Person> objectMap;

    public static final BiPredicate<List<?>, List<?>> LENIENT_SAME_ITEMS_ANY_ORDER =
            (expected, actual) -> {
                if (expected.size() != actual.size()) { // shortcut
                    return false;
                }
                List<Object[]> expectedList = (List<Object[]>) expected;
                List<Object[]> actualList = (List<Object[]>) actual;
                expectedList.sort(Comparator.comparingInt((Object[] o) -> (int) o[0]));
                actualList.sort(Comparator.comparingInt((Object[] o) -> (int) o[0]));
                for (int i = 0; i < expectedList.size(); i++) {
                    if (!Arrays.equals(expectedList.get(i), actualList.get(i))) {
                        return false;
                    }
                }
                return true;
            };

    @BeforeClass
    public static void setUp() {
        id = new AtomicInteger(0);
        initialize(1, null);
    }

    @Before
    public void before() {
        map = instance().getMap(randomMapName());
        final MapProxyImpl mapProxy = (MapProxyImpl) map;
        nodeEngine = ((MapService) mapProxy.getService()).getMapServiceContext().getNodeEngine();
    }

    @Test
    public void test_whenEmpty() {
        MapScanPlanNode scanNode = new MapScanPlanNode(
                id.getAndIncrement(),
                map.getName(),
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                Arrays.asList(QueryPath.KEY_PATH, valuePath("this")),
                Arrays.asList(QueryDataType.INT, QueryDataType.VARCHAR),
                Arrays.asList(0, 1),
                new ConstantPredicateExpression(true)
        );

        TestSupport
                .verifyProcessor(OnHeapMapScanP.ohHeapMapScanP(map.getName(), nodeEngine, scanNode))
                .jetInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(emptyList());
    }

    @Test
    public void test_whenNoFilterAndNoProjection() {
        List<Object[]> expected = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            map.put(i, "value-" + i);
            expected.add(new Object[]{i, "value-" + i});
        }

        MapScanPlanNode scanNode = new MapScanPlanNode(
                id.getAndIncrement(),
                map.getName(),
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                Arrays.asList(QueryPath.KEY_PATH, valuePath("this")),
                Arrays.asList(QueryDataType.INT, QueryDataType.VARCHAR),
                Arrays.asList(0, 1),
                new ConstantPredicateExpression(true)
        );

        TestSupport
                .verifyProcessor(OnHeapMapScanP.ohHeapMapScanP(map.getName(), nodeEngine, scanNode))
                .jetInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(LENIENT_SAME_ITEMS_ANY_ORDER)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(expected);
    }

    @Test
    public void test_whenFilterExistsAndNoProjection() {
        IMap<Integer, String> map = instance().getMap(randomMapName());
        for (int i = 0; i < 100; i++) {
            map.put(i, "value-" + i);
        }

        final MapProxyImpl mapProxy = (MapProxyImpl) map;
        final NodeEngine nodeEngine = ((MapService) mapProxy.getService()).getMapServiceContext().getNodeEngine();

        MapScanPlanNode scanNode = new MapScanPlanNode(
                id.getAndIncrement(),
                map.getName(),
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                Arrays.asList(QueryPath.KEY_PATH, valuePath("this")),
                Arrays.asList(QueryDataType.INT, QueryDataType.VARCHAR),
                Arrays.asList(0, 1),
                new ConstantPredicateExpression(false)
        );

        TestSupport
                .verifyProcessor(OnHeapMapScanP.ohHeapMapScanP(map.getName(), nodeEngine, scanNode))
                .jetInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(emptyList());
    }

    @Test
    public void test_whenNoFilterButProjectionExists() {
        objectMap = instance().getMap(randomMapName());

        List<Object[]> expected = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            objectMap.put(i, new Person("value-" + i, 100 - i));
            expected.add(new Object[]{i, "value-" + i, 100 - i});
        }

        MapScanPlanNode scanNode = new MapScanPlanNode(
                id.getAndIncrement(),
                objectMap.getName(),
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                Arrays.asList(QueryPath.KEY_PATH, valuePath("this.name"), valuePath("this.age")),
                Arrays.asList(QueryDataType.INT, QueryDataType.VARCHAR, QueryDataType.INT),
                Arrays.asList(0, 1, 2),
                new ConstantPredicateExpression(true)
        );

        TestSupport
                .verifyProcessor(OnHeapMapScanP.ohHeapMapScanP(objectMap.getName(), nodeEngine, scanNode))
                .jetInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(LENIENT_SAME_ITEMS_ANY_ORDER)
                .disableSnapshots()
                .disableProgressAssertion()
                .expectOutput(expected);
    }

    static class Person implements DataSerializable {
        private String name;
        private int age;

        Person() {
            // no op.
        }

        public String getName() {
            return name;
        }

        public int getAge() {
            return age;
        }

        Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(name);
            out.writeInt(age);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            this.name = in.readString();
            this.age = in.readInt();
        }
    }
}