plugins {
    id("com.palantir.git-version")
}
val gitVersion: groovy.lang.Closure<String> by extra
group = "io.whitefox"
version = gitVersion()