pipeline {
    agent any
    triggers {
        cron('H 4 * * *')
	pollSCM('H/2 * * * *') 
    }
    stages {
        stage('Build') {
            steps {
		sh 'ponyc -b pony-raft' 
                archiveArtifacts artifacts: 'pony-raft', fingerprint: true 
            }
        }
        stage('Test') {
            steps {
		sh './pony-raft' 
            }
        }
    }
}
