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
import org.apache.calcite.util.ReflectUtilUsingLambdaFactory;
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

import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaConversionException;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

/**
 * Benchmark for {@link org.apache.calcite.util.ReflectiveVisitDispatcherImpl}
 */
@Fork(value = 2, jvmArgsPrepend = "-Xmx1024m")
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class ReflectiveVisitDispatcherTest {

  private ReflectiveVisitor visitor = new People();

  @Benchmark
  public String testGlobalCaching() {
    ReflectUtil.MethodDispatcher<String> dispatcher = ReflectUtil.createMethodDispatcher(
        String.class, visitor, "say", String.class);
    String result = dispatcher.invoke("hello");
    return result;
  }

  @Benchmark
  public String testInstanceCachingWithReflection() {
    ReflectUtilOrigin.MethodDispatcher<String> dispatcher =
        ReflectUtilOrigin.createMethodDispatcher(
            String.class, visitor, "say", String.class);
    String result = dispatcher.invoke("hello");
    return result;
  }

  @Benchmark
  public String testInstanceCachingWithReflectionThreadLocalInitialize() throws NoSuchMethodException {
    if (ReflectUtilOrigin.BENCHMARK_THREADLOCAL.get() == null) {
      ReflectUtilOrigin.BENCHMARK_THREADLOCAL.set(People.class.getMethod("say", String.class));
    }
    ReflectUtilOrigin.MethodDispatcher<String> dispatcher =
        ReflectUtilOrigin.createMethodDispatcher(
            String.class, visitor, "say", String.class);
    String result = dispatcher.invoke("hello");
    return result;
  }

  @Benchmark
  public String testInstanceCachingWithMethodHandle() throws NoSuchMethodException, IllegalAccessException {
    ReflectUtilUsingMethodHandle.MethodDispatcher<String> dispatcher =
        ReflectUtilUsingMethodHandle.createMethodDispatcher(
            String.class, visitor, "say", String.class, String.class);
    String result = dispatcher.invoke("hello");
    return result;
  }

  @Benchmark
  public String testInstanceCachingWithMethodHandleThreadLocalInitialize() throws NoSuchMethodException, IllegalAccessException {
    if (ReflectUtilUsingMethodHandle.BENCHMARK_THREADLOCAL.get() == null) {
      MethodType mt = MethodType.methodType(String.class, String.class);
      ReflectUtilUsingMethodHandle.BENCHMARK_THREADLOCAL.set(MethodHandles.lookup().findVirtual(People.class, "say", mt));
    }
    ReflectUtilUsingMethodHandle.MethodDispatcher<String> dispatcher =
        ReflectUtilUsingMethodHandle.createMethodDispatcher(
            String.class, visitor, "say", String.class, String.class);
    String result = dispatcher.invoke("hello");
    return result;
  }

  @Benchmark
  public String testInstanceCachingWithLambdaFactory() throws NoSuchMethodException, IllegalAccessException {
    ReflectUtilUsingLambdaFactory.MethodDispatcher<String> dispatcher =
        ReflectUtilUsingLambdaFactory.createMethodDispatcher(
            String.class, visitor, "say", String.class, String.class);
    String result = dispatcher.invoke("hello");
    return result;
  }

  @Benchmark
  public String testInstanceCachingWithLambdaFactoryThreadLocalInitialize()
      throws NoSuchMethodException, IllegalAccessException, LambdaConversionException {
    if (ReflectUtilUsingLambdaFactory.BENCHMARK_THREADLOCAL.get() == null) {
      MethodType mt = MethodType.methodType(String.class, String.class);
      MethodHandle methodHandle = MethodHandles.lookup().findVirtual(People.class, "say", mt);

      CallSite callSite = LambdaMetafactory.metafactory(MethodHandles.lookup(),
          "apply",
          MethodType.methodType(BiFunction.class),
          MethodType.methodType(Object.class, Object.class, Object.class),
          methodHandle,
          MethodType.methodType(String.class, People.class, String.class));

      ReflectUtilUsingLambdaFactory.BENCHMARK_THREADLOCAL.set(callSite);
    }
    ReflectUtilUsingLambdaFactory.MethodDispatcher<String> dispatcher =
        ReflectUtilUsingLambdaFactory.createMethodDispatcher(
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
