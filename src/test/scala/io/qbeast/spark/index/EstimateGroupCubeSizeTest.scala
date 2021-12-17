package io.qbeast.spark.index

import org.scalatest.AppendedClues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class EstimateGroupCubeSizeTest extends AnyWordSpec with Matchers with AppendedClues {

  // numGroups = MAX(numPartitions, (numElements / cubeWeightsBufferCapacity))
  // groupCubeSize = desiredCubeSize / numGroups
  "EstimateGroupCubeSize" when {
    "numElements < desiredCubeSize and numElements < cubeWeightsBufferCapacity" should {
      "create the correct number of groups" in {
        DoublePassOTreeDataAnalyzer
          .estimateGroupCubeSize(
            desiredCubeSize = 1000000,
            numPartitions = 1,
            numElements = 10000,
            cubeWeightsBufferCapacity = 100000) shouldBe 1000000.0 +- 1
      }
    }

    "numElements == desiredCubeSize and numElements > cubeWeightsBufferCapacity" should {
      "create the correct number of groups for different number of partitions" in {
        val numPartitions = Seq(1, 10, 20)
        val groupCubeSizes = Seq(100000.0, 100000.0, 50000.0)

        for ((nP, gS) <- numPartitions zip groupCubeSizes) {
          DoublePassOTreeDataAnalyzer
            .estimateGroupCubeSize(
              desiredCubeSize = 1000000,
              numPartitions = nP,
              numElements = 1000000,
              cubeWeightsBufferCapacity =
                100000) shouldBe gS +- 1 withClue ("Number of partitions: " + nP)
        }
      }
    }

    "numElements > desiredCubeSize and numElements > cubeWeightsBufferCapacity" should {
      "create the correct number of groups" in {
        DoublePassOTreeDataAnalyzer.estimateGroupCubeSize(
          desiredCubeSize = 1000000,
          numPartitions = 15,
          numElements = 2000000,
          cubeWeightsBufferCapacity = 100000) shouldBe 50000.0 +- 1
      }
    }
  }

}
