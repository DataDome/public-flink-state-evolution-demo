/**
 * Not that all classes in the model are mutable and have a public no-argument constructor, because Flink only
 * recognises a type as a POJO (and therefore only uses {@code PojoSerializer} for it) under those conditions.
 * Instances should be treated as immutable once handed to Flink.
 */
package co.datadome.demo.flink.model;