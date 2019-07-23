package io.github.hbase4s.misc

import scala.util.control.NonFatal

object CloseableOps {

  def withCloseable[R, C <: { def close(): Unit }](resource: C)(block: C => R): R = {
    try {
      block(resource)
    } catch {
      case NonFatal(e) =>
        throw e
    } finally {
      if (resource != null) {
        resource.close()
      }
    }
  }
}