#
#   Copyright 2020 Logical Clocks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

from hsfs.engine import python_write, python_read, python_util


class Engine(python_write.EngineWrite, python_read.EngineRead, python_util.EngineUtil):
    def __init__(self):
        python_write.EngineWrite.__init__(self)
        python_read.EngineRead.__init__(self)
        python_util.EngineUtil.__init__(self)
