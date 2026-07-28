ThisBuild / scalaVersion := "3.5.2"

ThisBuild / scalacOptions ++= Seq(
  "-new-syntax",
  "-Wunused:all",
  "-deprecation",
  "-feature"
)

lazy val slussen = (project in file("."))
  .settings(
    name := "slussen",
    libraryDependencies ++= Seq(
      "ch.epfl.lamp" %% "gears" % "0.3.1",
      "io.lettuce" % "lettuce-core" % "6.5.0.RELEASE"
    )
  )
