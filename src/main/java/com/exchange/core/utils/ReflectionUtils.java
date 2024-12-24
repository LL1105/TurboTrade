package com.exchange.core.utils;

import java.lang.reflect.Field;

public final class ReflectionUtils {

    /**
     * 通过反射从指定对象中提取指定字段的值。
     * 
     * @param clazz 类的类型
     * @param object 目标对象
     * @param fieldName 字段名称
     * @param <R> 字段类型
     * @param <T> 对象类型
     * @return 字段的值
     * @throws IllegalStateException 如果无法访问字段
     */
    @SuppressWarnings(value = {"unchecked"})
    public static <R, T> R extractField(Class<T> clazz, T object, String fieldName) {
        try {
            // 获取指定字段对象
            final Field f = getField(clazz, fieldName);
            f.setAccessible(true); // 设置字段可访问（即使它是 private）
            // 获取字段值并强制转换为目标类型
            return (R) f.get(object);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            // 如果字段不存在或无法访问，抛出异常
            throw new IllegalStateException("Can not access Disruptor internals: ", e);
        }
    }

    /**
     * 通过反射从类中获取指定字段。如果字段不存在，则递归查找父类的字段。
     * 
     * @param clazz 类的类型
     * @param fieldName 字段名称
     * @return 字段对象
     * @throws NoSuchFieldException 如果字段不存在
     */
    public static Field getField(Class clazz, String fieldName) throws NoSuchFieldException {
        try {
            // 获取当前类的字段
            return clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            // 如果当前类没有该字段，查找父类
            Class superClass = clazz.getSuperclass();
            if (superClass == null) {
                // 如果没有父类，则抛出异常
                throw e;
            } else {
                // 递归查找父类中的字段
                return getField(superClass, fieldName);
            }
        }
    }
}
