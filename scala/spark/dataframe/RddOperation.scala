
#### Spark Group By Key to (Key,List) Pair
``` scala
data.groupByKey.mapValues(_.toList)

import scala.collection.JavaConverters._
def concatBookSimilarities(booksSimRdd: RDD[BookSimilarity], topK: Int): RDD[(Int, String)] = {
            val bookSimListRdd = booksSimRdd.groupBy(_.bookId1)
                .map {
                    case (bookId, bookSimilarities) =>
                        val topKSimList = bookSimilarities.toList
                            .sortWith(_.similarity > _.similarity)
                            .take(topK)
                            .asJava
                        (bookId, new Gson().toJson(topKSimList))
                }
            bookSimListRdd
        }

```

