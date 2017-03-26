package reductions

import common._
import org.scalameter._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig: MeasureBuilder[Unit, Double] = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> false
  ) withWarmer new Warmer.Default

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
    */
  def balance(chars: Array[Char]): Boolean = {
    var i = 0
    var stack = 0
    while (i < chars.length) {
      val char = chars(i)
      if (char == ')' && stack == 0) return false
      else if (char == '(') stack += 1
      else if (char == ')') stack -= 1
      i += 1
    }
    stack == 0
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
    */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {
//
//    def traverseRec(idx: Int, until: Int, unmatchedOpen: Int, unmatchedClose: Int): (Int, Int) /*: ???*/ = {
//      if (idx == until) (unmatchedOpen, unmatchedClose)
//      else {
//        val char = chars(idx)
//        char match {
//          case ')' if unmatchedOpen > 0 => traverseRec(idx + 1, until, unmatchedOpen - 1, unmatchedClose)
//          case '(' => traverseRec(idx + 1, until, unmatchedOpen + 1, unmatchedClose)
//          case ')' => traverseRec(idx + 1, until, unmatchedOpen, unmatchedClose + 1)
//          case _ => traverseRec(idx + 1, until, unmatchedOpen, unmatchedClose)
//        }
//      }
//    }

    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int): (Int, Int) /*: ???*/ = {
      var i = idx
      var unmatchedOpen = 0
      var unmatchedClose = 0
      while (i < until) {
        val char = chars(i)
        if (char == '(') unmatchedOpen += 1
        else if (char == ')' && unmatchedOpen > 0) unmatchedOpen -= 1
        else if (char == ')') unmatchedClose += 1
        i += 1
      }
      (unmatchedOpen, unmatchedClose)
    }

    def merge(left: (Int, Int), right: (Int, Int)): (Int, Int) = {
      // correspond to the unmatched open and closed parentheses counts for left and right
      // sub-problems respectively
      val (lOpen, lClosed) = left
      val (rOpen, rClosed) = right

      // all unmatched close parentheses in right-section covered by open-parentheses in left-section
      if (lOpen >= rClosed) {
        (lOpen - rClosed + rOpen, lClosed)
      }
      // all unmatched open parentheses in left-section covered by close-parentheses in right-section
      else {
        (rOpen, rClosed - lOpen + lClosed)
      }
    }

    def reduce(from: Int, until: Int): (Int, Int) /*: ???*/ = {
      if (until - from > threshold) {
        val split = (from + until) / 2
        val (l, r) = parallel(reduce(from, split), reduce(split, until))
        merge(l, r)
      } else {
        traverse(from, until, 0, 0)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
