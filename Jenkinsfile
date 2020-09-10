pipeline {
  agent any
  stages {

    stage('Prepare') {
      steps {
        sh 'mkdir -p arivi-core'
        dir(path: 'arivi-core') {
          git(url: 'https://github.com/xoken/arivi-core/', branch: 'master')
        }

        sh 'mkdir -p xoken-core'
        dir(path: 'xoken-core') {
          git(url: 'https://github.com/xoken/xoken-core/', branch: 'master')
        }

        sh 'mkdir -p vega'
        dir(path: 'vega') {
          git(url: 'https://github.com/xoken/vega/', branch: "${env.BRANCH_NAME}")
        }

      }
    }

    stage('Clean') {
      steps {
        dir(path: 'vega') {
          sh 'stack clean'
        }

      }
    }

    stage('Build') {
      steps {
        dir(path: 'vega') {
          sh 'stack install  --local-bin-path  ../build/reg/'
          sh 'stack install  --executable-profiling  --local-bin-path  ../build/prof/'
        }

        archiveArtifacts(artifacts: 'build/**/vega', followSymlinks: true)
      }
    }



      stage('Release') {
        

        steps {
          script {
            if ((env.BRANCH_NAME).startsWith("release")) {   
              echo '****** Starting Ubuntu18.04 container ******'
              dir(path: 'vega'){
                      sh 'rm -f /tmp/ubuntu1804.cid'
                      sh 'docker run -t -d --cidfile /tmp/ubuntu1804.cid -w  /opt/work/vega  vega/ubuntu18.04 sh'
                      sh 'docker exec -w /opt/work/arivi-core $(cat /tmp/ubuntu1804.cid) git pull'
                      sh 'docker exec -w /opt/work/xoken-core $(cat /tmp/ubuntu1804.cid) git pull'
                      sh 'docker exec -w /opt/work/vega $(cat /tmp/ubuntu1804.cid) git fetch '
                      sh 'docker exec -w /opt/work/vega $(cat /tmp/ubuntu1804.cid) git checkout $(basename $(git symbolic-ref HEAD))'
                      sh 'docker exec -w /opt/work/vega $(cat /tmp/ubuntu1804.cid) git pull'
                      sh 'docker exec -w /opt/work/vega $(cat /tmp/ubuntu1804.cid) stack clean'
                      sh 'docker exec -w /opt/work/vega $(cat /tmp/ubuntu1804.cid) stack install  --local-bin-path  . '
                      sh 'docker cp $(cat /tmp/ubuntu1804.cid):/opt/work/vega/vega  . '
                      sh 'rm -f /tmp/ubuntu1804.cid'
                      sh 'sha256sum ./vega > Checksum_SHA256'
                      sh 'zip vega_"$(basename $(git symbolic-ref HEAD))"_ubuntu1804.zip ./vega node-config.yaml ReleaseNotes README Checksum_SHA256 schema.cql LICENSE LICENSE-AGPL LICENSE-OpenBSV '
                    }
              echo '****** Starting Ubuntu20.04 container ******'
              dir(path: 'vega'){
                      sh 'rm -f /tmp/ubuntu2004.cid'
                      sh 'docker run -t -d --cidfile /tmp/ubuntu2004.cid -w  /opt/work/vega  vega/ubuntu20.04 sh'
                      sh 'docker exec -w /opt/work/arivi-core $(cat /tmp/ubuntu2004.cid) git pull'
                      sh 'docker exec -w /opt/work/xoken-core $(cat /tmp/ubuntu2004.cid) git pull'
                      sh 'docker exec -w /opt/work/vega $(cat /tmp/ubuntu2004.cid) git fetch '
                      sh 'docker exec -w /opt/work/vega $(cat /tmp/ubuntu2004.cid) git checkout $(basename $(git symbolic-ref HEAD))'
                      sh 'docker exec -w /opt/work/vega $(cat /tmp/ubuntu2004.cid) git pull'
                      sh 'docker exec -w /opt/work/vega $(cat /tmp/ubuntu2004.cid) stack clean'
                      sh 'docker exec -w /opt/work/vega $(cat /tmp/ubuntu2004.cid) stack install  --local-bin-path  . '
                      sh 'docker cp $(cat /tmp/ubuntu2004.cid):/opt/work/vega/vega  . '
                      sh 'rm -f /tmp/ubuntu2004.cid'
                      sh 'sha256sum ./vega > Checksum_SHA256'
                      sh 'zip vega_"$(basename $(git symbolic-ref HEAD))"_ubuntu2004.zip ./vega node-config.yaml ReleaseNotes README Checksum_SHA256 schema.cql LICENSE LICENSE-AGPL LICENSE-OpenBSV '
                    }
              echo '****** Starting Arch Linux container ******'
              dir(path: 'vega'){
                      sh 'rm -f /tmp/archlinux.cid'
                      sh 'docker run -t -d --cidfile /tmp/archlinux.cid -w  /opt/work/vega  vega/archlinux sh'
                      sh 'docker exec -w /opt/work/arivi-core $(cat /tmp/archlinux.cid) git pull'
                      sh 'docker exec -w /opt/work/xoken-core $(cat /tmp/archlinux.cid) git pull'
                      sh 'docker exec -w /opt/work/vega $(cat /tmp/archlinux.cid) git fetch '
                      sh 'docker exec -w /opt/work/vega $(cat /tmp/archlinux.cid) git checkout $(basename $(git symbolic-ref HEAD))'
                      sh 'docker exec -w /opt/work/vega $(cat /tmp/archlinux.cid) git pull'
                      sh 'docker exec -w /opt/work/vega $(cat /tmp/archlinux.cid) stack clean'
                      sh 'docker exec -w /opt/work/vega $(cat /tmp/archlinux.cid) env LD_PRELOAD=/usr/lib/libjemalloc.so.2 stack install  --local-bin-path  . '
                      sh 'docker cp $(cat /tmp/archlinux.cid):/opt/work/vega/vega  . '
                      sh 'rm -f /tmp/archlinux.cid'
                      sh 'sha256sum ./vega > Checksum_SHA256'
                      sh 'zip vega_"$(basename $(git symbolic-ref HEAD))"_archlinux.zip ./vega node-config.yaml ReleaseNotes README Checksum_SHA256 schema.cql LICENSE LICENSE-AGPL LICENSE-OpenBSV '
                    }
                    archiveArtifacts(artifacts: 'vega/vega*.zip', followSymlinks: true)
          } else { 
          echo 'skipping Docker release packaging..'
          }
        }
        } 
        
       } 

    } 
    
  
      post {
          unsuccessful {
                  emailext(subject: '$PROJECT_NAME - Build # $BUILD_NUMBER - $BUILD_STATUS!', body: '$PROJECT_NAME - Build # $BUILD_NUMBER - $BUILD_STATUS    ||   Please check attached logfile for more details.', attachLog: true, from: 'buildmaster@xoken.org', replyTo: 'buildmaster@xoken.org', to: 'jenkins-notifications@xoken.org')
           
          }
          fixed {
                  emailext(subject: '$PROJECT_NAME - Build # $BUILD_NUMBER - $BUILD_STATUS!', body: '$PROJECT_NAME - Build # $BUILD_NUMBER - $BUILD_STATUS  ||  Previous build was not successful and the current builds status is SUCCESS ||  Please check attached logfile for more details.', attachLog: true, from: 'buildmaster@xoken.org', replyTo: 'buildmaster@xoken.org', to: 'jenkins-notifications@xoken.org')
          }
      }
  
  
}
