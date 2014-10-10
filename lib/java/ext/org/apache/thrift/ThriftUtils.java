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
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.annotation.ThriftField;
import org.apache.thrift.annotation.ThriftStruct;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;

/**
 * Generic serializer and deserializer based on Java annotations. 
 * 
 * @author Emmanuel Bourg
 * @version $Revision$, $Date$
 */
public class ThriftUtils {

    private static Map<Class, Field[]> cache = new HashMap<Class, Field[]>(); 

    private static Map<Class, Map<Short, Field>> fieldmapCache = new HashMap<Class, Map<Short, Field>>(); 

    private static Field[] getFields(Class cls) {
        Field[] fields = cache.get(cls);
        if (fields == null) {
            fields = cls.getFields();
            cache.put(cls, fields);
        }
        
        return fields;
    }

    /**
     * Deserializes an object using the specified protocol.
     * 
     * @param iprot  the input protocol
     * @param cls    the class of the object
     * @throws TException
     */
    public static Object read(TProtocol iprot, Class<?> cls) throws TException {
        try {
            if (String.class.equals(cls)) {
                return iprot.readString();
            } else if (Number.class.isAssignableFrom(cls)) {
                if (Byte.class.equals(cls)) {
                    return iprot.readByte();
                } else if (Short.class.equals(cls)) {
                    return iprot.readI16();
                } else if (Integer.class.equals(cls)) {
                    return iprot.readI32();
                } else if (Long.class.equals(cls)) {
                    return iprot.readI64();
                } else if (Double.class.equals(cls)) {
                    return iprot.readDouble();
                } else {
                    throw new RuntimeException("Unsupported type: " + cls);
                }
            } else if (Boolean.class.equals(cls)) {
                return iprot.readBool();
            } else if (cls.isEnum()) {
                int value = iprot.readI32();
                return cls.getMethod("findByValue", int.class).invoke(null, value);
            } else {
                Object obj = cls.newInstance();
                read(iprot, obj);
                return obj;
            }
        } catch (Exception e) {
            throw new TException("Couldn't read " + cls + " instance", e);
        }
    }

    /**
     * Returns the map of fields for the class specified. The key is the id of the Thrift field.
     */
    private static Map<Short, Field> getFieldMap(Class cls) {
        Map<Short, Field> fields = fieldmapCache.get(cls);
        if (fields != null) {
            return fields;
        }
        
        fields = new HashMap<Short, Field>();
        for (Field field : getFields(cls)) {
            ThriftField desc = field.getAnnotation(ThriftField.class);
            if (desc != null) {
                fields.put(desc.id(), field);
            }
        }
        
        fieldmapCache.put(cls, fields);
        
        return fields;
    }

    /**
     * Deserializes a Thrift struct using the specified protocol.
     * 
     * @param iprot   the input protocol
     * @param object  the object to serialize
     * @throws TException
     */
    public static <T> void read(TProtocol iprot, T object) throws TException {
        iprot.readStructBegin();
        
        Map<Short, Field> fields = getFieldMap(object.getClass());

        while (true) {
            TField field = iprot.readFieldBegin();
            if (field.type == TType.STOP) {
                break;
            }

            if (!fields.keySet().contains(field.id)) {
                TProtocolUtil.skip(iprot, field.type);
                continue;
            }

            try {
                Object value;
                
                Field f = fields.get(field.id);

                switch (field.type) {
                    case TType.BOOL:
                        value = iprot.readBool();
                        break;
                    case TType.BYTE:
                        value = iprot.readByte();
                        break;
                    case TType.DOUBLE:
                        value = iprot.readDouble();
                        break;
                    case TType.I16:
                        value = iprot.readI16();
                        break;
                    case TType.I32:
                        value = iprot.readI32();
                        
                        // detect enums
                        if (f.getType().isEnum()) {
                            Method findByValue = f.getType().getMethod("findByValue", int.class);
                            value = findByValue.invoke(null, value);
                        }                        
                        break;
                    
                    case TType.I64:
                        value = iprot.readI64();
                        break;
                    case TType.STRING:
                        value = iprot.readString();
                        break;
                    
                    case TType.STRUCT:
                        value = read(iprot, f.getType());
                        break;
                    
                    case TType.MAP:                        
                        value = readMap(iprot, (ParameterizedType) f.getGenericType());
                        break;
                        
                    case TType.SET:                        
                        value = readSet(iprot, (ParameterizedType) f.getGenericType());
                        break;
                        
                    case TType.LIST:                        
                        value = readList(iprot, (ParameterizedType) f.getGenericType());
                        break;

                    case TType.ENUM:                        
                        // doesn't happen, enums are transported as integers (i32)
                        
                    case TType.VOID:
                    default:
                        throw new TException("Unsupported type: " + field);
                }
                
                f.set(object, value);
                
            } catch (TException e) {
                throw e;
            } catch (Exception e) {
                throw new TException(e);
            }
        }
        
        iprot.readStructEnd();
    }

    private static Object read(TProtocol iprot, Type type) throws TException {
        if (type instanceof Class) {
            return read(iprot, (Class) type);
        } else if (type instanceof ParameterizedType) {
            Class cls = (Class) ((ParameterizedType) type).getRawType();
            if (Map.class.isAssignableFrom(cls)) {
                return readMap(iprot, (ParameterizedType) type);
            } else if (List.class.isAssignableFrom(cls)) {
                return readList(iprot, (ParameterizedType) type);
            } else if (Set.class.isAssignableFrom(cls)) {
                return readSet(iprot, (ParameterizedType) type);
            }
        }

        throw new IllegalArgumentException("Unsupported type: " + type);
    }

    @SuppressWarnings("unchecked")
    private static List readList(TProtocol iprot, ParameterizedType type) throws TException {
        Class cls = (Class) type.getActualTypeArguments()[0];
        
        TList tlist = iprot.readListBegin();
        List list = new ArrayList(tlist.size);
        
        for (int i = 0; i < tlist.size; ++i) {
            list.add(read(iprot, cls));
        }
        
        iprot.readListEnd();
        
        return list;
    }

    @SuppressWarnings("unchecked")
    private static Set readSet(TProtocol iprot, ParameterizedType type) throws TException {
        Type elementType = type.getActualTypeArguments()[0];
        
        TSet tset = iprot.readSetBegin();
        Set set = new HashSet(tset.size);
        
        for (int i = 0; i < tset.size; ++i) {
            set.add(read(iprot, elementType));
        }
        
        iprot.readSetEnd();
        
        return set;
    }

    @SuppressWarnings("unchecked")
    private static Map readMap(TProtocol iprot, ParameterizedType type) throws TException {
        Type keyType = type.getActualTypeArguments()[0];
        Type valueType = type.getActualTypeArguments()[1];
        
        TMap tmap = iprot.readMapBegin();
        Map map = new HashMap(tmap.size);
        for (int i = 0; i < tmap.size; ++i) {
            Object key = read(iprot, keyType);
            Object val = read(iprot, valueType);
            
            map.put(key, val);
        }
        iprot.readMapEnd();
        
        return map;
    }
    
    /**
     * Serializes a Thrift struct using the specified protocol.
     * 
     * @param oprot   the output protocol
     * @param object  the object to serialize
     * @throws TException
     */
    public static void write(TProtocol oprot, Object object) throws TException {
        if (object instanceof String) {
            oprot.writeString((String) object);
            return;
        } else if (object instanceof Number) {
            if (object instanceof Byte) {
                oprot.writeByte((Byte) object);
                return;
            } else if (object instanceof Short) {
                oprot.writeI16((Short) object);
                return;
            } else if (object instanceof Integer) {
                oprot.writeI32((Integer) object);
                return;
            } else if (object instanceof Long) {
                oprot.writeI64((Long) object);
                return;
            } else if (object instanceof Double) {
                 oprot.writeDouble((Double) object);
                return;
            } else {
                throw new RuntimeException("Unsupported type: " + object.getClass());
            }
        } else if (object instanceof Boolean) {
            oprot.writeBool((Boolean) object);
            return;
        }
        
        ThriftStruct struct = object.getClass().getAnnotation(ThriftStruct.class);
        if (struct == null) {
            throw new IllegalArgumentException("Object is not a Thrift struct: " + object);
        }
        
        try {
            oprot.writeStructBegin(new TStruct(struct.value()));

            for (Field field : getFields(object.getClass())) {
                ThriftField desc = field.getAnnotation(ThriftField.class);
                if (desc != null) {
                    Object value = field.get(object);
                    if (value != null) {
                        oprot.writeFieldBegin(new TField(desc.name(), desc.type(), desc.id()));
                        switch (desc.type()) {
                            case TType.BOOL:
                                oprot.writeBool((Boolean) value);
                                break;
                            case TType.BYTE:
                                oprot.writeByte((Byte) value);
                                break;
                            case TType.DOUBLE:
                                oprot.writeDouble((Double) value);
                                break;
                            case TType.I16:
                                oprot.writeI16((Short) value);
                                break;
                            case TType.I32:
                                // handle enums
                                if (field.getType().isEnum()) {
                                    Method getValue = field.getType().getMethod("getValue");
                                    oprot.writeI32((Integer) getValue.invoke(value));
                                } else {
                                    oprot.writeI32((Integer) value);
                                }

                                break;
                            case TType.I64:
                                oprot.writeI64((Long) value);
                                break;
                            case TType.STRING:
                                oprot.writeString((String) value);
                                break;

                            case TType.STRUCT:
                                write(oprot, value);
                                break;

                            case TType.MAP:
                                // todo fix nested collections
                                Map map = (Map) value;
                                Class keyClass = (Class) ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
                                Class valueClass = (Class) ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[1];

                                TMap tmap = new TMap(getThriftType(keyClass), getThriftType(valueClass), map.size());
                                oprot.writeMapBegin(tmap);
                                Set<Map.Entry> entries = map.entrySet();
                                for (Map.Entry entry : entries) {
                                    write(oprot, entry.getKey()); // NPE if key is null
                                    write(oprot, entry.getValue());
                                }
                                
                                oprot.writeMapEnd();
                                break;

                            case TType.SET:
                                // todo fix nested collections
                                Set set = (Set) value;
                                Class elementClass = (Class) ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];

                                TSet tset = new TSet(getThriftType(elementClass), set.size());
                                oprot.writeSetBegin(tset);
                                
                                for (Object element : set) {
                                    write(oprot, element);
                                }

                                oprot.writeSetEnd();
                                break;

                            case TType.LIST:
                                // todo fix nested collections
                                List list = (List) value;
                                Class cls = (Class) ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];

                                TList tlist = new TList(getThriftType(cls), list.size());
                                oprot.writeListBegin(tlist);

                                for (Object element : list) {
                                    write(oprot, element);
                                }

                                oprot.writeListEnd();
                                break;

                            case TType.ENUM:
                                // doesn't happen, enums are transported as integers (i32)

                            case TType.VOID:
                            default:
                                throw new TException("Unsupported type: " + field);
                        }


                        oprot.writeFieldEnd();
                    }
                }
            }

            oprot.writeFieldStop();
            oprot.writeStructEnd();

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new TException(e);
        }
    }

    /**
     * Returns the Thrift type (TType) equivalent to the specified class.
     * 
     * @param cls
     */
    public static byte getThriftType(Class cls) {
        if (String.class.equals(cls)) {
            return TType.STRING;
        } else if (Number.class.isAssignableFrom(cls)) {
            if (Byte.class.equals(cls)) {
                return  TType.BYTE;
            } else if (Short.class.equals(cls)) {
                return  TType.I16;
            } else if (Integer.class.equals(cls)) {
                return  TType.I32;
            } else if (Long.class.equals(cls)) {
                return  TType.I64;
            } else if (Double.class.equals(cls)) {
                return  TType.DOUBLE;
            } else {
                throw new IllegalArgumentException("Unsupported type: " + cls);
            }
        } else if (Boolean.class.equals(cls)) {
            return  TType.BOOL;
        } else if (Map.class.isAssignableFrom(cls)) {
            return TType.MAP;
        } else if (Set.class.isAssignableFrom(cls)) {
            return TType.SET;
        } else if (List.class.isAssignableFrom(cls)) {
            return TType.LIST;
        } else {
            return TType.STRUCT;
        }
    }
}
