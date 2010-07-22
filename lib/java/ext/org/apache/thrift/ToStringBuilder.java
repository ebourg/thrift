/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.thrift;

import java.lang.reflect.Field;

/**
 * Utility class for building toString() results for Thrift structs.
 * 
 * @author Emmanuel Bourg
 * @version $Revision$, $Date$
 */
public class ToStringBuilder {

    public static String toString(Object instance) {
        StringBuilder sb = new StringBuilder(instance.getClass().getSimpleName()).append("(");
        Field[] fields = instance.getClass().getFields();
        boolean first = true;
        for (Field field : fields) {
            if ((field.getModifiers() & java.lang.reflect.Modifier.STATIC) == 0) {
                if (!first) {
                    sb.append(", ");
                }
                sb.append(field.getName());
                sb.append(":");
                try {
                    sb.append(field.get(instance));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
                first = false;
            }
        }
        sb.append(")");
        return sb.toString();
    }

}
