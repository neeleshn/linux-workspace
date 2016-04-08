
lazy val root = (project in file(".")).
    settings(
        name := "Hall of Fame",
        libraryDependencies += ("org.apache.spark" %% "spark-core" % "1.5.2")
    )
