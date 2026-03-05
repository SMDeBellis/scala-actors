package actors

/**
 * Unique identifier for an actor within an ActorSystem
 */
case class ActorPath(systemName: String, segments: Seq[String]) {

  def name: String = segments.lastOption.getOrElse("")

  def parent: Option[ActorPath] = {
    if (segments.length <= 1) None
    else Some(ActorPath(systemName, segments.init))
  }

  def child(name: String): ActorPath = {
    ActorPath(systemName, segments :+ name)
  }

  override def toString: String = {
    segments.mkString(s"akka://${systemName}/", "/", "")
  }

  def absolute: String = "/" + segments.mkString("/")
}

object ActorPath {
  def root(systemName: String): ActorPath = ActorPath(systemName, Nil)

  def apply(systemName: String, path: String): ActorPath = {
    val segments = path.stripPrefix("/").split("/").filter(_.nonEmpty)
    ActorPath(systemName, segments)
  }
}
