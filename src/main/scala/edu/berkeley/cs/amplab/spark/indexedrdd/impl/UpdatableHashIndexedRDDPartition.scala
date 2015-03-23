/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.cs.amplab.spark.indexedrdd.impl

import scala.collection.immutable.LongMap
import scala.reflect.ClassTag

import org.apache.spark.Logging

import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDDPartition
import edu.berkeley.cs.amplab.spark.indexedrdd.util.BitSet
import edu.berkeley.cs.amplab.spark.indexedrdd.util.ImmutableBitSet
import edu.berkeley.cs.amplab.spark.indexedrdd.util.ImmutableLongOpenHashSet
import edu.berkeley.cs.amplab.spark.indexedrdd.util.ImmutableVector
import edu.berkeley.cs.amplab.spark.indexedrdd.util.OpenHashSet
import edu.berkeley.cs.amplab.spark.indexedrdd.util.PrimitiveKeyOpenHashMap
import edu.berkeley.cs.amplab.spark.indexedrdd.util.PrimitiveVector

private[indexedrdd] class NonUpdatableHashIndexedRDDPartition[
    @specialized(Long, Int, Double) K,
    @specialized(Long, Int, Double) V](
    protected val index: OpenHashSet[K],
    protected val values: Array[V],
    protected val mask: BitSet)(
    override implicit val kTag: ClassTag[K],
    override implicit val vTag: ClassTag[V])
  extends IndexedRDDPartition[K, V] with Logging {

  protected def withIndex(index: OpenHashSet[K]): NonUpdatableHashIndexedRDDPartition[K, V] = {
    new NonUpdatableHashIndexedRDDPartition(index, values, mask)
  }

  protected def withValues[V2: ClassTag](values: Array[V2]): NonUpdatableHashIndexedRDDPartition[K, V2] = {
    new NonUpdatableHashIndexedRDDPartition(index, values, mask)
  }

  protected def withMask(mask: BitSet): NonUpdatableHashIndexedRDDPartition[K, V] = {
    new NonUpdatableHashIndexedRDDPartition(index, values, mask)
  }

  protected def capacity: Int = index.capacity

  override def size: Int = mask.cardinality()

  override def apply(k: K): V = values(index.getPos(k))

  override def isDefined(k: K): Boolean = {
    val pos = index.getPos(k)
    pos >= 0 && mask.get(pos)
  }

  override def iterator: Iterator[(K, V)] =
    mask.iterator.map(ind => (index.getValue(ind), values(ind)))

  override def multiget(ks: Iterator[K]): Iterator[Option[(K, V)]] = {
    ks.map { k => if (self.isDefined(k)) Some(this(k)) else None }
  }

  override def mapValues[V2: ClassTag](f: (K, V) => V2): IndexedRDDPartition[K, V2] = {
    val newValues = new Array[V2](capacity)
    mask.iterator.foreach { i =>
      newValues(i) = f(index.getValue(i), values(i))
    }
    this.withValues(newValues)
  }

  override def filter(pred: (K, V) => Boolean): IndexedRDDPartition[K, V] = {
    val newMask = new BitSet(self.capacity)
    mask.iterator.foreach { i =>
      if (pred(index.getValue(i), values(i))) {
        newMask.set(i)
      }
    }
    this.withMask(newMask)
  }

  override def fullOuterJoin[V2: ClassTag, W: ClassTag]
      (other: IndexedRDDPartition[K, V2])
      (f: (K, Option[V], Option[V2]) => W): IndexedRDDPartition[K, W] = other match {
    case other: UpdatableHashIndexedRDDPartition[K, V2] if index == other.index =>
      val newValues = new Array[W](capacity)
      val newMask = mask | other.mask

      (mask & other.mask).iterator.foreach { i =>
        newValues(i) = f(index.getValue(i), Some(values(i)), Some(other.values(i)))
      }
      mask.andNot(other.mask).iterator.foreach { i =>
        newValues(i) = f(index.getValue(i), Some(values(i)), None)
      }
      other.mask.andNot(mask).iterator.foreach { i =>
        newValues(i) = f(index.getValue(i), None, Some(other.values(i)))
      }

      this.withValues(newValues).withMask(newMask)

    case _ =>
      fullOuterJoin(other.iterator)(f)
  }

  override def fullOuterJoin[V2: ClassTag, W: ClassTag]
      (other: Iterator[(K, V2)])
      (f: (K, Option[V], Option[V2]) => W): IndexedRDDPartition[K, W] = ???

  override def union[U: ClassTag]
      (other: IndexedRDDPartition[K, U])
      (f: (K, V, U) => V): IndexedRDDPartition[K, V] = other match {
    case other: UpdatableHashIndexedRDDPartition[K, U] if index == other.index =>
      val newValues = new Array[V](capacity)
      System.arraycopy(values, 0, newValues, 0, capacity)

      (mask & other.mask).iterator.foreach { i =>
        newValues(i) = f(index.getValue(i), values(i), other.values(i))
      }

      this.withValues(newValues)

    case _ =>
      union(other.iterator)(f)
  }

  override def union[U: ClassTag]
      (other: Iterator[(K, U)])
      (f: (K, V, U) => V): IndexedRDDPartition[K, V] = ???

  override def leftOuterJoin[V2: ClassTag, V3: ClassTag]
      (other: IndexedRDDPartition[K, V2])
      (f: (K, V, Option[V2]) => V3): IndexedRDDPartition[K, V3] = other match {
    case other: UpdatableHashIndexedRDDPartition[K, V2] if index == other.index =>
      val newValues = new Array[V3](self.capacity)

      mask.iterator.foreach { i =>
        val otherV: Option[V2] = if (other.mask.get(i)) Some(other.values(i)) else None
        newValues(i) = f(index.getValue(i), values(i), otherV)
      }
      this.withValues(ImmutableVector.fromArray(newValues))
    }
  }

  /** Left outer joins `this` with the iterator `other`, running `f` on all values of `this`. */
  def leftJoin[V2: ClassTag, V3: ClassTag]
      (other: Iterator[(K, V2)])
      (f: (K, V, Option[V2]) => V3): IndexedRDDPartition[K, V3] = {
    leftJoin(createUsingIndex(other))(f)
  }

  /** Inner joins `this` with `other`, running `f` on the values of corresponding keys. */
  def innerJoin[U: ClassTag, V2: ClassTag]
      (other: IndexedRDDPartition[K, U])
      (f: (K, V, U) => V2): IndexedRDDPartition[K, V2] = {
    if (self.index != other.index) {
      logWarning("Joining two IndexedRDDPartitions with different indexes is slow.")
      val newMask = new BitSet(self.capacity)
      val newValues = new Array[V2](self.capacity)

      self.mask.iterator.foreach { i =>
        val vid = self.index.getValue(i)
        val otherI =
          if (other.index.getValue(i) == vid) {
            if (other.mask.get(i)) i else -1
          } else {
            if (other.isDefined(vid)) other.index.getPos(vid) else -1
          }
        if (otherI != -1) {
          newValues(i) = f(vid, self.values(i), other.values(otherI))
          newMask.set(i)
        }
      }
      this.withValues(ImmutableVector.fromArray(newValues)).withMask(newMask.toImmutableBitSet)
    } else {
      val newMask = self.mask & other.mask
      val newValues = new Array[V2](self.capacity)
      newMask.iterator.foreach { i =>
        newValues(i) = f(self.index.getValue(i), self.values(i), other.values(i))
      }
      this.withValues(ImmutableVector.fromArray(newValues)).withMask(newMask)
    }
  }

  /**
   * Inner joins `this` with the iterator `other`, running `f` on the values of corresponding
   * keys.
   */
  def innerJoin[U: ClassTag, V2: ClassTag]
      (iter: Iterator[Product2[K, U]])
      (f: (K, V, U) => V2): IndexedRDDPartition[K, V2] = {
    innerJoin(createUsingIndex(iter))(f)
  }

  /**
   * Inner joins `this` with `iter`, taking values from `iter` and hiding other values using the
   * bitmask.
   */
  def innerJoinKeepLeft(iter: Iterator[Product2[K, V]]): IndexedRDDPartition[K, V] = {
    val newMask = new BitSet(self.capacity)
    var newValues = self.values
    iter.foreach { pair =>
      val pos = self.index.getPos(pair._1)
      if (pos >= 0) {
        newMask.set(pos)
        newValues = newValues.updated(pos, pair._2)
      }
    }
    this.withValues(newValues).withMask(newMask.toImmutableBitSet)
  }

  /**
   * Creates a new IndexedRDDPartition with values from `iter` that may share an index with `this`,
   * merging duplicate keys in `messages` arbitrarily. If `iter` contains keys not in the index of
   * `this`, the new index will be different.
   */
  def createUsingIndex[V2: ClassTag](iter: Iterator[Product2[K, V2]])
    : IndexedRDDPartition[K, V2] = {
    aggregateUsingIndex(iter, (a, b) => b)
  }

  /**
   * Creates a new IndexedRDDPartition with values from `iter` that may share an index with `this`,
   * merging duplicate keys using `reduceFunc`. If `iter` contains keys not in the index of `this`,
   * the new index will be different.
   */
  def aggregateUsingIndex[V2: ClassTag](
      iter: Iterator[Product2[K, V2]],
      reduceFunc: (V2, V2) => V2): IndexedRDDPartition[K, V2] = {
    val newMask = new BitSet(self.capacity)
    val newValues = new Array[V2](self.capacity)
    val newElements = new PrimitiveVector[Product2[K, V2]]
    iter.foreach { product =>
      val id = product._1
      val value = product._2
      val pos = self.index.getPos(id)
      if (pos >= 0) {
        if (newMask.get(pos)) {
          newValues(pos) = reduceFunc(newValues(pos), value)
        } else { // otherwise just store the new value
          newMask.set(pos)
          newValues(pos) = value
        }
      } else {
        newElements += product
      }
    }

    val aggregated = this.withValues(ImmutableVector.fromArray(newValues))
      .withMask(newMask.toImmutableBitSet)
    if (newElements.length > 0) {
      val newElementsIter = newElements.trim().array.iterator
      aggregated.multiputIterator(newElementsIter, (id, a, b) => throw new Exception(
        "merge function was called but newElementsIter should only contain new elements"))
    } else {
      aggregated
    }
  }

  /**
   * Rebuilds the indexes of this IndexedRDDPartition, removing deleted entries. The resulting
   * IndexedRDDPartition will not support efficient joins with the original one.
   */
  def reindex(): IndexedRDDPartition[K, V] = {
    val hashMap = new PrimitiveKeyOpenHashMap[K, V]
    val arbitraryMerge = (a: V, b: V) => a
    for ((k, v) <- self.iterator) {
      hashMap.setMerge(k, v, arbitraryMerge)
    }
    this.withIndex(ImmutableLongOpenHashSet.fromLongOpenHashSet(hashMap.keySet))
      .withValues(ImmutableVector.fromArray(hashMap.values))
      .withMask(hashMap.keySet.getBitSet.toImmutableBitSet)
  }
}
