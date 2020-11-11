pipeline {
    agent {
        docker {
            label "local"
            image "docker.hops.works/hopsworks_twine:0.0.1"
        }
    }
    stages {
        stage("publish") {
            environment {
                PYPI = credentials('977daeb0-e1c8-43a0-b35a-fc37bb9eee9b')
            }
            steps {
                dir("python") {
                    sh "rm -f LICENSE README.md"
                    sh "cp -f ../LICENSE ../README.md ./"
                    sh "python3 ./setup.py sdist"
                    sh "twine upload -u $PYPI_USR -p $PYPI_PSW --skip-existing dist/*"
                }
            }
        }
    }
}
