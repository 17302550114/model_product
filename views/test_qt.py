
import sys

from PyQt5.QtWidgets import QApplication, QLabel

app = QApplication(sys.argv)

label = QLabel("Hello PyQt5!")

label.show()

sys.exit(app.exec_())

#保存并关闭文件后，在终端中运行以下命令来执行脚本：

