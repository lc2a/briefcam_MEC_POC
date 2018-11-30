import unittest
import os

#Get the parent directory.
dirname = '/'.join(os.path.dirname(os.path.realpath(__file__)).split('/')[:-1])
print(dirname)


tests =unittest.TestLoader().discover(dirname,
                         pattern='test*.py')
result = unittest.TextTestRunner(verbosity=2).run(tests)

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