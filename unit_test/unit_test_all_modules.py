import unittest
import os

#Get the parent directory.
dirname = '/'.join(os.path.dirname(os.path.realpath(__file__)).split('/')[:-1])
print(dirname)

#method 1. Just point the discover to the root parent directory.
# It will automatically search for unit test files with the specified pattern and execute them.
# Note that if the global modules are not available on the local host, then the import will fail the test case.
#tests =unittest.TestLoader().discover(dirname,
#                         pattern='test*.py')
#result = unittest.TextTestRunner(verbosity=2).run(tests)

#method 2. Just point it to the directory where you have the unit test file name starting with test*.py format.
#tests =unittest.TestLoader().discover('/home/sriramsridhar/git/briefcam_MEC_POC/tier3/machine_learning_workers/unittest')
#tests =unittest.TestLoader().discover('/home/sriramsridhar/git/briefcam_MEC_POC/tier3/job_dispatcher/unittest')
#tests =unittest.TestLoader().discover('/home/sriramsridhar/git/briefcam_MEC_POC/tier2/rtsp_operate_media/unittest')
#tests =unittest.TestLoader().discover('/home/sriramsridhar/git/briefcam_MEC_POC/tier1/rtsp_recorder_orchestrator/unittest')
#tests =unittest.TestLoader().discover('/home/sriramsridhar/git/briefcam_MEC_POC/tier1/front_end/unittest')
#tests =unittest.TestLoader().discover('/home/sriramsridhar/git/briefcam_MEC_POC/infrastructure_components/producer_consumer/wurstmeister_kafka_msgq_api/unittest')
#tests =unittest.TestLoader().discover('/home/sriramsridhar/git/briefcam_MEC_POC/infrastructure_components/producer_consumer/unittest')
#tests =unittest.TestLoader().discover('/home/sriramsridhar/git/briefcam_MEC_POC/infrastructure_components/data_parser/briefcam_parser/unittest')
#tests =unittest.TestLoader().discover('/home/sriramsridhar/git/briefcam_MEC_POC/infrastructure_components/data_parser/unittest')
#tests =unittest.TestLoader().discover('/home/sriramsridhar/git/briefcam_MEC_POC/infrastructure_components/couchdb_client/unittest')
#tests =unittest.TestLoader().discover('/home/sriramsridhar/git/briefcam_MEC_POC/infrastructure_components/redisClient/unittest')

#result = unittest.TextTestRunner(verbosity=2).run(tests)

#method 3. Build a docker image based upon the Dockerfile found in each submodule and unit test it.
#Cleaner and better approach.
tests =unittest.TestLoader().discover('/home/sriramsridhar/git/briefcam_MEC_POC/tier3/job_dispatcher/unittest',
                                      pattern='docker_build_ut_publish.py')
result = unittest.TextTestRunner(verbosity=2).run(tests)
