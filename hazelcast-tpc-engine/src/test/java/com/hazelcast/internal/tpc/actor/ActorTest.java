/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpc.actor;


import com.hazelcast.internal.tpc.nio.NioEventloop;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.tpc.TpcTestSupport.assertOpenEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ActorTest {

    private NioEventloop eventloop;

    @Before
    public void before() {
        eventloop = new NioEventloop();
        eventloop.start();
    }

    @After
    public void after() throws InterruptedException {
        if (eventloop != null) {
            eventloop.shutdown();
            assertTrue(eventloop.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test(expected = IllegalStateException.class)
    public void test_activate_whenAlreadyActivated() {
        Actor actor = new Actor() {
            @Override
            public void process(Object msg) {
            }
        };
        actor.activate(eventloop);
        actor.activate(eventloop);
    }

    @Test
    public void test_receiveMsg() {
        CountDownLatch executed = new CountDownLatch(1);
        AtomicReference msgRef = new AtomicReference();
        Actor actor = new Actor() {
            @Override
            public void process(Object msg) {
                msgRef.set(msg);
                executed.countDown();
            }
        };
        ActorRef ref = actor.handle();
        actor.activate(eventloop);
        String msg = "Message";
        ref.send(msg);
        assertOpenEventually(executed);
        assertEquals(msg, msgRef.get());
    }
}
