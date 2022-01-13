pipeline {
	agent any
	triggers {
		cron('H 4 * * *')
	pollSCM('H/2 * * * *') 
	}
	stages {
		stage('Tooling') {
			steps {
				sh 'ponyc --version'
			}
		}
		stage('Build Release') {
			steps {
				sh 'ponyc -b pony-raft'
				archiveArtifacts artifacts: 'pony-raft', fingerprint: true
			}
		}
		stage('Build Debug') {
			steps {
				sh 'ponyc -d -b pony-raft-debug'
				archiveArtifacts artifacts: 'pony-raft-debug', fingerprint: true
			}
		}
		stage('Test Release') {
			steps {
				sh './pony-raft'
			}
		}
		stage('Test Debug') {
			steps {
				sh './pony-raft-debug'
			}
		}
	}

	/* set up notifications */
	post {
		always {
			echo 'One way or another, I have finished'
			/* deleteDir() */ /* clean up our workspace */
		}
		success {
			echo 'I succeeeded!'
		}
		unstable {
			echo 'I am unstable :/'
			mail to: 'sgebbie+jenkins@gethos.net',
			  subject: "Pipeline unstable: ${currentBuild.fullDisplayName}",
			  body: "The build is unstable: ${env.BUILD_URL}"
		}
		failure {
			echo 'I failed :('
			mail to: 'sgebbie+jenkins@gethos.net',
			  subject: "Pipeline failed: ${currentBuild.fullDisplayName}",
			  body: "The build has failed: ${env.BUILD_URL}"
		}
		changed {
			echo 'Things were different before...'
			mail to: 'sgebbie+jenkins@gethos.net',
			  subject: "Pipeline status changed: ${currentBuild.fullDisplayName} now ${currentBuild.result}",
			  body: "The status of the build has changed: ${env.BUILD_URL}"
		}
	}
}
