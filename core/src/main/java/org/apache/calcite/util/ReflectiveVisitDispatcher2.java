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
package org.apache.calcite.util;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Interface for looking up methods relating to reflective visitation. One
 * possible implementation would cache the results.
 *
 * <p>Type parameter 'R' is the base class of visitoR class; type parameter 'E'
 * is the base class of visiteE class.
 *
 * <p>TODO: obsolete {@link ReflectUtil#lookupVisitMethod}, and use caching in
 * implementing that method.
 *
 * @param <E> Argument type
 * @param <R> Return type
 */
public interface ReflectiveVisitDispatcher2<R extends ReflectiveVisitor, E> {
  //~ Methods ----------------------------------------------------------------

  /**
   * Looks up a visit method taking additional parameters beyond the
   * overloaded visitee type.
   *
   * @param visitorClass             class of object whose visit method is to be
   *                                 invoked
   * @param visiteeClass             class of object to be passed as a parameter
   *                                 to the visit method
   * @param visitMethodName          name of visit method
   * @param additionalParameterTypes list of additional parameter types
   * @return method found, or null if none found
   */
  MethodHandle lookupVisitMethod(
      Class<? extends R> visitorClass,
      Class<? extends E> visiteeClass,
      String visitMethodName,
      Class returnType,
      List<Class> additionalParameterTypes);

  /**
   * Looks up a visit method.
   *
   * @param visitorClass    class of object whose visit method is to be invoked
   * @param visiteeClass    class of object to be passed as a parameter to the
   *                        visit method
   * @param visitMethodName name of visit method
   * @return method found, or null if none found
   */
  MethodHandle lookupVisitMethod(
      Class<? extends R> visitorClass,
      Class<? extends E> visiteeClass,
      String visitMethodName,
      Class returnType);

}
