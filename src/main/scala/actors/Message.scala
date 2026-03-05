package actors

/**
 * Type alias for messages
 */
type Message = AnyRef

/**
 * Simple mutable reference wrapper
 */
class Ref[T](private var value: T) {
  def get(): T = value
  def set(newValue: T): Unit = { value = newValue }
  def compareAndSet(expected: T, newValue: T): Boolean = {
    if (value == expected) {
      value = newValue
      true
    } else {
      false
    }
  }
}
