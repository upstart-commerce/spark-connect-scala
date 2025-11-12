ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.4"
ThisBuild / organization := "org.apache.spark"

lazy val root = (project in file("."))
  .settings(
    name := "spark-connect-scala",

    // Scala compiler options for Scala 3
    scalacOptions ++= Seq(
      "-encoding", "utf8",
      "-feature",
      "-unchecked",
      "-language:higherKinds",
      "-language:implicitConversions"
    ),

    // Dependency versions
    libraryDependencies ++= {
      val catsEffectVersion = "3.5.4"
      val fs2Version = "3.10.2"
      val grpcVersion = "1.68.1"
      val scalapbVersion = scalapb.compiler.Version.scalapbVersion
      val arrowVersion = "18.1.0"
      val logbackVersion = "1.5.12"

      Seq(
        // Cats Effect for functional programming
        "org.typelevel" %% "cats-effect" % catsEffectVersion,
        "org.typelevel" %% "cats-core" % "2.12.0",

        // FS2 for streaming
        "co.fs2" %% "fs2-core" % fs2Version,
        "co.fs2" %% "fs2-io" % fs2Version,

        // gRPC and Protocol Buffers
        "io.grpc" % "grpc-netty-shaded" % grpcVersion,
        "io.grpc" % "grpc-protobuf" % grpcVersion,
        "io.grpc" % "grpc-stub" % grpcVersion,
        "com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion % "protobuf",
        "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapbVersion,

        // Apache Arrow for efficient data transfer
        "org.apache.arrow" % "arrow-vector" % arrowVersion,
        "org.apache.arrow" % "arrow-memory-netty" % arrowVersion,
        "org.apache.arrow" % "arrow-format" % arrowVersion,

        // Logging
        "ch.qos.logback" % "logback-classic" % logbackVersion,
        "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",

        // Testing
        "org.scalatest" %% "scalatest" % "3.2.19" % Test,
        "org.typelevel" %% "cats-effect-testing-scalatest" % "1.5.0" % Test,
        "org.scalatestplus" %% "mockito-5-12" % "3.2.19.0" % Test
      )
    },

    // ScalaPB settings for protobuf code generation
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = true) -> (Compile / sourceManaged).value / "scalapb"
    ),

    // Add protobuf source directory
    Compile / PB.protoSources := Seq(
      baseDirectory.value / "src" / "main" / "protobuf"
    ),

    // Ensure protobuf files are included in the package
    Compile / unmanagedResourceDirectories += baseDirectory.value / "src" / "main" / "protobuf"
  )
