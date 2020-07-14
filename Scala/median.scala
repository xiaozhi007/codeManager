  //求中位数
  def median(input :Array[Int]) : Int = {
    val l = input.length
    val l_2 = l/2.toInt
    val x = l%2
    var y = 0
    if(x == 0) {
      y = ( input(l_2) + input(l_2 + 1))/2
    }else {
      y = (input(l_2))
    }
    return y
  }//median