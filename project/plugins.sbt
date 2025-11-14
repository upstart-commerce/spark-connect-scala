// ScalaPB plugin for Protocol Buffers code generation
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.7")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.17"

// Scoverage plugin for code coverage
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.9")
