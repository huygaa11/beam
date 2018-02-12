/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.direct;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import org.apache.beam.runners.core.ReadyCheckingSideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.UsesStatefulParDo;
import org.apache.beam.sdk.testing.UsesTestStream;
import org.apache.beam.sdk.testing.UsesTimersInParDo;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;

/** Tests for {@link StatefulParDoEvaluatorFactory}. */
@RunWith(JUnit4.class)
public class ParDoTest implements Serializable {
  @Mock private transient EvaluationContext mockEvaluationContext;
  @Mock private transient DirectExecutionContext mockExecutionContext;
  @Mock private transient DirectExecutionContext.DirectStepContext mockStepContext;
  @Mock private transient ReadyCheckingSideInputReader mockSideInputReader;
  @Mock private transient UncommittedBundle<Integer> mockUncommittedBundle;

  private static final String KEY = "any-key";
  private transient StateInternals stateInternals =
      CopyOnAccessInMemoryStateInternals.<Object>withUnderlying(KEY, null);

  private static final BundleFactory BUNDLE_FACTORY = ImmutableListBundleFactory.create();

  @Rule
  public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  @Category({ValidatesRunner.class, UsesStatefulParDo.class, UsesTimersInParDo.class, UsesTestStream.class})
  public void testValueStateSimple() {
    final String stateId = "foo";
    final String timerId = "bar";

    DoFn<KV<String, Integer>, Integer> fn =
        new DoFn<KV<String, Integer>, Integer>() {

          @TimerId(timerId)
          private final TimerSpec timer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

          @StateId(stateId)
          private final StateSpec<ValueState<Integer>> intState =
              StateSpecs.value(VarIntCoder.of());


          @ProcessElement
          public void processElement(
              ProcessContext c, @StateId(stateId) ValueState<Integer> state,
              @TimerId(timerId) Timer timer) {
            Integer currentValue = MoreObjects.firstNonNull(state.read(), 0);
            c.output(currentValue);
            state.write(currentValue + 1);

            timer.offset(Duration.standardSeconds(1)).setRelative();
          }

          @OnTimer(timerId)
          public void onTimer(BoundedWindow w, @StateId(stateId) ValueState<Integer> state) {
            System.out.println(state.read());
            System.out.println("timer");
          }
        };

    TestStream<KV<String, Integer>> stream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
            .addElements(KV.of("key", 1))
            .addElements(KV.of("key", 2))
            .addElements(KV.of("key", 3))
            .advanceProcessingTime(Duration.standardSeconds(2))
            .advanceWatermarkToInfinity();

    PCollection<Integer> output = pipeline.apply(stream).apply(ParDo.of(fn));

    PAssert.that(output).containsInAnyOrder(0, 1, 2);
    pipeline.run();
  }

  @Test
  @Category({ValidatesRunner.class, UsesStatefulParDo.class, UsesTimersInParDo.class, UsesTestStream.class})
  public void testValueStateSimpleUnbounded() {
    final String stateId = "foo";
    final String timerId = "bar";

    DoFn<KV<String, Integer>, Integer> fn =
        new DoFn<KV<String, Integer>, Integer>() {

          @TimerId(timerId)
          private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);
          //
          @StateId(stateId)
          private final StateSpec<ValueState<Integer>> intState =
              StateSpecs.value(VarIntCoder.of());


          @ProcessElement
          public void processElement(
              ProcessContext c
              ,@StateId(stateId) ValueState<Integer> state
              ,@TimerId(timerId) Timer timer
          ) {
            Integer currentValue = MoreObjects.firstNonNull(state.read(), -10);
            state.write(currentValue + 1);

            // c.output(currentValue);
            // timer.align(Duration.standardDays(10)).setRelative();
          }

          @OnTimer(timerId)
          public void onTimer(
              OnWindowExpirationContext c,
              @StateId(stateId) ValueState<Integer> state) {
            System.out.println("timer state: " + state.read());
            // c.output(999);
          }

          @OnWindowExpiration
          public void bla(
              OnWindowExpirationContext c,
              @StateId(stateId) ValueState<Integer> state) {
            System.out.println("window state: " + state.read());
            c.output(100);
          }
        };

    DoFn<Integer, Integer> fn1 =
        new DoFn<Integer, Integer>() {
          @ProcessElement
          public void processElement(
              ProcessContext c) {
            System.out.println("passed element: " + c.element());
            System.out.println("passed timestamp: " + c.timestamp());
            c.output(c.element());
          }
        };

    TestStream<KV<String, Integer>> stream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
            // .advanceWatermarkTo(BoundedWindow.TIMESTAMP_MAX_VALUE.minus(Duration.standardDays(1)))
            .advanceWatermarkTo(new Instant(0))
            .addElements(KV.of("key", 1))
            // .addElements(KV.of("key", 2))
            // .addElements(KV.of("key", 3))
            .advanceWatermarkTo(new Instant(0).plus(Duration.standardDays(8)))
            .advanceWatermarkTo(new Instant(0).plus(Duration.standardDays(20)))
            .advanceWatermarkToInfinity();

    PCollection<Integer> output = pipeline.apply(stream)
        .apply("first", ParDo.of(fn))
        .apply("second", ParDo.of(fn1));

    PAssert.that(output).containsInAnyOrder(100);
    pipeline.run();
  }
}
