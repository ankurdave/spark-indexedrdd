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

package edu.berkeley.cs.amplab.spark.indexedrdd

import scala.collection.immutable.LongMap
import scala.language.higherKinds
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * An RDD of key-value `(K, V)` pairs that pre-indexes the entries for fast lookups, joins, and
 * optionally updates. To construct an `IndexedRDD`, use one of the constructors in the
 * [[edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD$ IndexedRDD object]].
 *
 * @tparam K the key associated with each entry in the set.
 * @tparam V the value associated with each entry in the set.
 */
class IndexedRDD[K: ClassTag, V: ClassTag](
    /** The underlying representation of the IndexedRDD as an RDD of partitions. */
    protected val partitionsRDD: RDD[IndexedRDDPartition[K, V]])
  extends RDD[(K, V)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  require(partitionsRDD.partitioner.isDefined)

  override val partitioner = partitionsRDD.partitioner

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override protected def getPreferredLocations(s: Partition): Seq[String] =
    partitionsRDD.preferredLocations(s)

  override def persist(newLevel: StorageLevel): this.type = {
    partitionsRDD.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): this.type = {
    partitionsRDD.unpersist(blocking)
    this
  }

  override def count(): Long = {
    partitionsRDD.map(_.size).reduce(_ + _)
  }

  /** Provides the `RDD[(K, V)]` equivalent output. */
  override def compute(part: Partition, context: TaskContext): Iterator[(K, V)] = {
    firstParent[IndexedRDDPartition[K, V]].iterator(part, context).next.iterator
  }

  /** Gets the value corresponding to the specified key, if any. */
  def get(k: K): Option[V] = multiget(Array(k)).get(k)

  /** Gets the values corresponding to the specified keys, if any. */
  def multiget(ks: Array[K]): Map[K, V] = {
    val ksByPartition = ks.groupBy(k => self.partitioner.get.getPartition(k))
    val partitions = ksByPartition.keys.toSeq
    // TODO: avoid sending all keys to all partitions by creating and zipping an RDD of keys
    val results: Array[Array[(K, V)]] = self.context.runJob(self.partitionsRDD,
      (context: TaskContext, partIter: Iterator[IndexedRDDPartition[K, V]]) => {
        if (partIter.hasNext && ksByPartition.contains(context.partitionId)) {
          val part = partIter.next()
          val ksForPartition = ksByPartition.get(context.partitionId).get
          part.multiget(ksForPartition).toArray
        } else {
          Array.empty
        }
      }, partitions, allowLocal = true)
    results.flatten.toMap
  }

  /**
   * Unconditionally updates the specified key to have the specified value. Returns a new IndexedRDD
   * that reflects the modification.
   */
  def put(k: K, v: V): IndexedRDD[K, V] = multiput(Map(k -> v))

  /**
   * Unconditionally updates the keys in `kvs` to their corresponding values. Returns a new
   * IndexedRDD that reflects the modification.
   */
  def multiput(kvs: Map[K, V]): IndexedRDD[K, V] = multiput(kvs, (id, a, b) => b)

  /**
   * Updates the keys in `kvs` to their corresponding values, running `merge` on old and new values
   * if necessary. Returns a new IndexedRDD that reflects the modification.
   */
  def multiput(kvs: Map[K, V], merge: (K, V, V) => V): IndexedRDD[K, V] = {
    val updates = self.context.parallelize(kvs.toSeq).partitionBy(self.partitioner.get)
    zipPartitionsWithOther(updates)(new MultiputZipper(merge))
  }

  /** Deletes the specified keys. Returns a new IndexedRDD that reflects the deletions. */
  def delete(ks: Array[K]): IndexedRDD[K, V] = {
    val deletions = self.context.parallelize(ks.map(k => (k, ()))).partitionBy(self.partitioner.get)
    zipPartitionsWithOther(deletions)(new DeleteZipper)
  }

  /** Applies a function to each partition of this IndexedRDD. */
  protected def mapIndexedRDDPartitions[K2: ClassTag, V2: ClassTag](
      f: IndexedRDDPartition[K, V] => IndexedRDDPartition[K2, V2]): IndexedRDD[K2, V2] = {
    val newPartitionsRDD = self.partitionsRDD.mapPartitions(_.map(f), preservesPartitioning = true)
    new IndexedRDD(newPartitionsRDD)
  }

  /** Applies a function to corresponding partitions of `this` and another IndexedRDD. */
  protected def zipIndexedRDDPartitions[V2: ClassTag, V3: ClassTag](other: IndexedRDD[K, V2])
      (f: ZipPartitionsFunction[V2, V3]): IndexedRDD[K, V3] = {
    assert(self.partitioner == other.partitioner)
    val newPartitionsRDD = self.partitionsRDD.zipPartitions(other.partitionsRDD, true)(f)
    new IndexedRDD(newPartitionsRDD)
  }

  /** Applies a function to corresponding partitions of `this` and a pair RDD. */
  protected def zipPartitionsWithOther[V2: ClassTag, V3: ClassTag](other: RDD[(K, V2)])
      (f: OtherZipPartitionsFunction[V2, V3]): IndexedRDD[K, V3] = {
    val partitioned = other.partitionBy(self.partitioner.get)
    val newPartitionsRDD = self.partitionsRDD.zipPartitions(partitioned, true)(f)
    new IndexedRDD(newPartitionsRDD)
  }

  /**
   * Restricts the entries to those satisfying the given predicate. This operation preserves the
   * index for efficient joins with the original IndexedRDD and is implemented using soft deletions.
   *
   * @param pred the user defined predicate, which takes a tuple to conform to the `RDD[(K, V)]`
   * interface
   */
  override def filter(pred: Tuple2[K, V] => Boolean): IndexedRDD[K, V] =
    this.mapIndexedRDDPartitions(_.filter(Function.untupled(pred)))

  /** Maps each value, preserving the index. */
  def mapValues[V2: ClassTag](f: V => V2): IndexedRDD[K, V2] =
    this.mapIndexedRDDPartitions(_.mapValues((vid, attr) => f(attr)))

  /** Maps each value, supplying the corresponding key and preserving the index. */
  def mapValues[V2: ClassTag](f: (K, V) => V2): IndexedRDD[K, V2] =
    this.mapIndexedRDDPartitions(_.mapValues(f))

  /**
   * Intersects `this` and `other` and keeps only elements with differing values. For these
   * elements, keeps the values from `this`.
   */
  def diff(other: IndexedRDD[K, V]): IndexedRDD[K, V] =
    this.zipIndexedRDDPartitions(other)(new DiffZipper)

  /**
   * Joins `this` with `other`, running `f` on the values of all keys in both sets. Note that for
   * efficiency `other` must be an IndexedRDD, not just a pair RDD. Use [[aggregateUsingIndex]] to
   * construct an IndexedRDD co-partitioned with `this`.
   */
  def fullOuterJoin[V2: ClassTag, W: ClassTag]
      (other: IndexedRDD[K, V2])
      (f: (K, Option[V], Option[V2]) => W): IndexedRDD[K, W] = {
    require(self.partitioner == other.partitioner)
    this.zipIndexedRDDPartitions(other)(new FullOuterJoinZipper(f))
  }

  /**
   * Left outer joins `this` with `other`, running `f` on the values of corresponding keys. Because
   * values in `this` with no corresponding entries in `other` are preserved, `f` cannot change the
   * value type.
   */
  def join[U: ClassTag]
      (other: RDD[(K, U)])(f: (K, V, U) => V): IndexedRDD[K, V] = other match {
    case other: IndexedRDD[K, U] if self.partitioner == other.partitioner =>
      this.zipIndexedRDDPartitions(other)(new JoinZipper(f))
    case _ =>
      this.zipPartitionsWithOther(other)(new OtherJoinZipper(f))
  }

  /** Left outer joins `this` with `other`, running `f` on all values of `this`. */
  def leftJoin[V2: ClassTag, V3: ClassTag]
      (other: RDD[(K, V2)])(f: (K, V, Option[V2]) => V3): IndexedRDD[K, V3] = other match {
    case other: IndexedRDD[K, V2] if self.partitioner == other.partitioner =>
      this.zipIndexedRDDPartitions(other)(new LeftJoinZipper(f))
    case _ =>
      this.zipPartitionsWithOther(other)(new OtherLeftJoinZipper(f))
  }

  /** Inner joins `this` with `other`, running `f` on the values of corresponding keys. */
  def innerJoin[V2: ClassTag, V3: ClassTag](other: RDD[(K, V2)])
      (f: (K, V, V2) => V3): Self[V3] = other match {
    case other: IndexedRDD[K, V2] if self.partitioner == other.partitioner =>
      this.zipIndexedRDDPartitions(other)(new InnerJoinZipper(f))
    case _ =>
      this.zipPartitionsWithOther(other)(new OtherInnerJoinZipper(f))
  }

  /**
   * Creates a new IndexedRDD with values from `messages` that shares an index with `this`, merging
   * duplicate keys in `messages` arbitrarily.
   */
  def createUsingIndex[V2: ClassTag](
      messages: RDD[(K, V2)]): IndexedRDD[K, V2] = {
    this.zipPartitionsWithOther(messages)(new CreateUsingIndexZipper)
  }

  /** Creates a new IndexedRDD with values from `messages` that shares an index with `this`. */
  def aggregateUsingIndex[V2: ClassTag](
      messages: RDD[(K, V2)], reduceFunc: (V2, V2) => V2): IndexedRDD[K, V2] = {
    this.zipPartitionsWithOther(messages)(new AggregateUsingIndexZipper(reduceFunc))
  }

  /**
   * Rebuilds the indexes of this IndexedRDD, removing deleted entries. The resulting IndexedRDD
   * will not support efficient joins with the original one.
   */
  def reindex(): IndexedRDD[K, V] = new IndexedRDD(self.partitionsRDD.map(_.reindex()))

  // The following functions could have been anonymous, but we name them to work around a Scala
  // compiler bug related to specialization.

  protected type ZipPartitionsFunction[V2, V3] =
    Function2[Iterator[IndexedRDDPartition[K, V]], Iterator[IndexedRDDPartition[K, V2]],
      Iterator[IndexedRDDPartition[K, V3]]]

  protected type OtherZipPartitionsFunction[V2, V3] =
    Function2[Iterator[IndexedRDDPartition[K, V]], Iterator[(K, V2)],
      Iterator[IndexedRDDPartition[K, V3]]]

  private class MultiputZipper(merge: (K, V, V) => V)
      extends OtherZipPartitionsFunction[V, V] with Serializable {
    def apply(thisIter: Iterator[IndexedRDDPartition[K, V]], otherIter: Iterator[(K, V)])
      : Iterator[IndexedRDDPartition[K, V]] = {
      val thisPart = thisIter.next()
      val updates = otherIter.toSeq
      Iterator(thisPart.multiput(updates, merge))
    }
  }

  private class DeleteZipper extends OtherZipPartitionsFunction[Unit, V] with Serializable {
    def apply(thisIter: Iterator[IndexedRDDPartition[K, V]], otherIter: Iterator[(K, Unit)])
      : Iterator[IndexedRDDPartition[K, V]] = {
      val thisPart = thisIter.next()
      val deletions = otherIter.map(_._1).toArray
      Iterator(thisPart.delete(deletions))
    }
  }

  private class DiffZipper extends ZipPartitionsFunction[V, V] with Serializable {
    def apply(
        thisIter: Iterator[IndexedRDDPartition[K, V]], otherIter: Iterator[IndexedRDDPartition[K, V]])
      : Iterator[IndexedRDDPartition[K, V]] = {
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.diff(otherPart))
    }
  }

  private class FullOuterJoinZipper[V2: ClassTag, W: ClassTag](f: (K, Option[V], Option[V2]) => W)
      extends ZipPartitionsFunction[V2, W] with Serializable {
    def apply(
        thisIter: Iterator[IndexedRDDPartition[K, V]], otherIter: Iterator[IndexedRDDPartition[K, V2]])
        : Iterator[IndexedRDDPartition[K, W]] = {
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.fullOuterJoin(otherPart)(f))
    }
  }

  private class JoinZipper[U: ClassTag](f: (K, V, U) => V)
      extends ZipPartitionsFunction[U, V] with Serializable {
    def apply(
        thisIter: Iterator[IndexedRDDPartition[K, V]], otherIter: Iterator[IndexedRDDPartition[K, U]])
      : Iterator[IndexedRDDPartition[K, V]] = {
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.join(otherPart)(f))
    }
  }

  private class OtherJoinZipper[U: ClassTag](f: (K, V, U) => V)
      extends OtherZipPartitionsFunction[U, V] with Serializable {
    def apply(thisIter: Iterator[IndexedRDDPartition[K, V]], otherIter: Iterator[(K, U)])
      : Iterator[IndexedRDDPartition[K, V]] = {
      val thisPart = thisIter.next()
      Iterator(thisPart.join(otherIter)(f))
    }
  }

  private class LeftJoinZipper[V2: ClassTag, V3: ClassTag](f: (K, V, Option[V2]) => V3)
      extends ZipPartitionsFunction[V2, V3] with Serializable {
    def apply(
        thisIter: Iterator[IndexedRDDPartition[K, V]], otherIter: Iterator[IndexedRDDPartition[K, V2]])
      : Iterator[IndexedRDDPartition[K, V3]] = {
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.leftJoin(otherPart)(f))
    }
  }

  private class OtherLeftJoinZipper[V2: ClassTag, V3: ClassTag](f: (K, V, Option[V2]) => V3)
      extends OtherZipPartitionsFunction[V2, V3] with Serializable {
    def apply(thisIter: Iterator[IndexedRDDPartition[K, V]], otherIter: Iterator[(K, V2)])
      : Iterator[IndexedRDDPartition[K, V3]] = {
      val thisPart = thisIter.next()
      Iterator(thisPart.leftJoin(otherIter)(f))
    }
  }

  private class InnerJoinZipper[V2: ClassTag, V3: ClassTag](f: (K, V, V2) => V3)
      extends ZipPartitionsFunction[V2, V3] with Serializable {
    def apply(
        thisIter: Iterator[IndexedRDDPartition[K, V]], otherIter: Iterator[IndexedRDDPartition[K, V2]])
      : Iterator[IndexedRDDPartition[K, V3]] = {
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.innerJoin(otherPart)(f))
    }
  }

  private class OtherInnerJoinZipper[V2: ClassTag, V3: ClassTag](f: (K, V, V2) => V3)
      extends OtherZipPartitionsFunction[V2, V3] with Serializable {
    def apply(thisIter: Iterator[IndexedRDDPartition[K, V]], otherIter: Iterator[(K, V2)])
      : Iterator[IndexedRDDPartition[K, V3]] = {
      val thisPart = thisIter.next()
      Iterator(thisPart.innerJoin(otherIter)(f))
    }
  }

  private class CreateUsingIndexZipper[V2: ClassTag]
      extends OtherZipPartitionsFunction[V2, V2] with Serializable {
    def apply(thisIter: Iterator[IndexedRDDPartition[K, V]], otherIter: Iterator[(K, V2)]): Iterator[IndexedRDDPartition[K, V2]] = {
      val thisPart = thisIter.next()
      Iterator(thisPart.createUsingIndex(otherIter))
    }
  }

  private class AggregateUsingIndexZipper[V2: ClassTag](reduceFunc: (V2, V2) => V2)
      extends OtherZipPartitionsFunction[V2, V2] with Serializable {
    def apply(thisIter: Iterator[IndexedRDDPartition[K, V]], otherIter: Iterator[(K, V2)])
      : Iterator[IndexedRDDPartition[K, V2]] = {
      val thisPart = thisIter.next()
      Iterator(thisPart.aggregateUsingIndex(otherIter, reduceFunc))
    }
  }
}

object IndexedRDD {
  /**
   * Constructs an IndexedRDD from an RDD of pairs, arbitrarily merging duplicate keys within the same partition.
   */
  def immutableHash[K: ClassTag, V: ClassTag](elems: RDD[(K, V)]): IndexedRDD[K, V] = {
    IndexedRDD.immutableHash(elems, (a, b) => b)
  }

  /** Constructs an IndexedRDD from an RDD of pairs. */
  def immutableHash[V: ClassTag](
      elems: RDD[(K, V)], partitioner: Partitioner, mergeValues: (V, V) => V): IndexedRDD[V] = {
    val partitions = elems.mapPartitions(
      iter => Iterator(IndexedRDDPartition(iter, mergeValues)), preservesPartitioning = true)
    new IndexedRDD(partitions)
  }
}
