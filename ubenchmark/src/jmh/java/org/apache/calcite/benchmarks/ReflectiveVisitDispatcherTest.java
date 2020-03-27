/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.benchmarks;

import org.apache.calcite.benchmarks.helper.People;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectUtilOrigin;
import org.apache.calcite.util.ReflectUtilUsingMethodHandle;
import org.apache.calcite.util.ReflectiveVisitor;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark for {@link org.apache.calcite.util.ReflectiveVisitDispatcherImpl}
 */
@Fork(value = 1, jvmArgsPrepend = "-Xmx1024m")
@Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class ReflectiveVisitDispatcherTest {

  private ReflectiveVisitor visitor = new People();

  @Benchmark
  public String testReflectiveVisitorDispatcherInvokeGlobalCaching() {
    ReflectUtil.MethodDispatcher<String> dispatcher = ReflectUtil.createMethodDispatcher(
        String.class, visitor, "say", String.class);
    String result = dispatcher.invoke("hello");
    return result;
  }

  @Benchmark
  public String testReflectiveVisitorDispatcherInvokeInstanceCaching() {
    ReflectUtilOrigin.MethodDispatcher<String> dispatcher =
        ReflectUtilOrigin.createMethodDispatcher(
            String.class, visitor, "say", String.class);
    String result = dispatcher.invoke("hello");
    return result;
  }

  @Benchmark
  public String testReflectiveVisitorDispatcherInvokeInstanceCachingUseMethodHandle() throws NoSuchMethodException, IllegalAccessException {
    ReflectUtilUsingMethodHandle.MethodDispatcher<String> dispatcher =
        ReflectUtilUsingMethodHandle.createMethodDispatcher(
            String.class, visitor, "say", String.class, String.class);
    String result = dispatcher.invoke("hello");
    return result;
  }

  public static void main(String[] args) throws RunnerException, NoSuchMethodException, IllegalAccessException {
    Options opt = new OptionsBuilder()
        .include(ReflectiveVisitDispatcherTest.class.getSimpleName())
        .build();

    new Runner(opt).run();
  }

}
