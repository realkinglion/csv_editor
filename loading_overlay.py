# loading_overlay.py - 修正版
from PySide6.QtWidgets import QWidget, QLabel, QVBoxLayout, QProgressBar
from PySide6.QtCore import Qt, QTimer, QPropertyAnimation, QRect, Property, QEvent # QEvent をインポート
from PySide6.QtGui import QPainter, QColor, QPalette

class LoadingOverlay(QWidget):
    """軽量で即座に表示されるローディングオーバーレイ"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAttribute(Qt.WA_TransparentForMouseEvents, False)
        self.setAttribute(Qt.WA_StyledBackground, True)
        
        # 半透明の背景
        self.setStyleSheet("""
            LoadingOverlay {
                background-color: rgba(0, 0, 0, 180);
            }
        """)
        
        # メインコンテナ
        self.container = QWidget(self)
        self.container.setFixedSize(300, 200)
        self.container.setStyleSheet("""
            QWidget {
                background-color: #ffffff;
                border-radius: 10px;
                padding: 20px;
            }
        """)
        
        # レイアウト
        layout = QVBoxLayout(self.container)
        layout.setAlignment(Qt.AlignCenter)
        
        # アニメーションローダー（軽量なQtネイティブ実装）
        self.spinner = CircularSpinner(self.container)
        self.spinner.setFixedSize(60, 60)
        layout.addWidget(self.spinner, alignment=Qt.AlignCenter)
        
        # ステータステキスト
        self.status_label = QLabel("ファイルを読み込んでいます...")
        self.status_label.setAlignment(Qt.AlignCenter)
        self.status_label.setStyleSheet("""
            QLabel {
                font-size: 14px;
                color: #333333;
                margin-top: 10px;
            }
        """)
        layout.addWidget(self.status_label)
        
        # プログレスバー（オプション）
        self.progress_bar = QProgressBar()
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                border: 1px solid #cccccc;
                border-radius: 5px;
                height: 10px;
                background-color: #f0f0f0;
            }
            QProgressBar::chunk {
                background-color: #2E86C1;
                border-radius: 4px;
            }
        """)
        self.progress_bar.hide()  # 初期は非表示
        layout.addWidget(self.progress_bar)
        
        # フェードインアニメーション
        self.fade_animation = QPropertyAnimation(self, b"windowOpacity")
        self.fade_animation.setDuration(200)
        self.fade_animation.setStartValue(0.0)
        self.fade_animation.setEndValue(1.0)
        
        # 親ウィジェットのリサイズに追従
        if parent:
            parent.installEventFilter(self)
    
    def showEvent(self, event):
        """表示時にセンタリングとアニメーション開始"""
        super().showEvent(event)
        self._center_container()
        self.spinner.start()
        self.fade_animation.start()
    
    def hideEvent(self, event):
        """非表示時にアニメーション停止"""
        super().hideEvent(event)
        self.spinner.stop()
    
    def _center_container(self):
        """コンテナを中央に配置"""
        if self.parent():
            parent_rect = self.parent().rect()
            x = (parent_rect.width() - self.container.width()) // 2
            y = (parent_rect.height() - self.container.height()) // 2
            self.container.move(x, y)
    
    def eventFilter(self, obj, event):
        """親ウィジェットのリサイズを監視"""
        # 🔥 修正: event.type()をQEvent.Type.Resizeと正しく比較
        if obj == self.parent() and event.type() == QEvent.Resize: #
            self.resize(self.parent().size())
            self._center_container()
        return super().eventFilter(obj, event)
    
    def set_status(self, text):
        """ステータステキストを更新"""
        self.status_label.setText(text)
    
    def show_progress(self, show=True):
        """プログレスバーの表示/非表示"""
        self.progress_bar.setVisible(show)
    
    def set_progress(self, value, maximum=100):
        """プログレスバーの値を設定"""
        self.progress_bar.setMaximum(maximum)
        self.progress_bar.setValue(value)


class CircularSpinner(QWidget):
    """軽量な円形スピナーアニメーション"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self._angle = 0
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._rotate)
        
    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        
        # 円の描画設定
        pen_width = 4
        painter.setPen(Qt.NoPen)
        
        # 背景の円
        painter.setBrush(QColor(240, 240, 240))
        painter.drawEllipse(pen_width, pen_width, 
                            self.width() - 2*pen_width, 
                            self.height() - 2*pen_width)
        
        # 回転する弧
        painter.setBrush(QColor(46, 134, 193))  # #2E86C1
        painter.translate(self.width() / 2, self.height() / 2)
        painter.rotate(self._angle)
        
        # 3つの点を描画
        for i in range(3):
            painter.rotate(120)
            opacity = 1.0 - (i * 0.3)
            color = QColor(46, 134, 193)
            color.setAlphaF(opacity)
            painter.setBrush(color)
            painter.drawEllipse(-20, -4, 8, 8)
    
    def _rotate(self):
        """回転アニメーション"""
        self._angle = (self._angle + 10) % 360
        self.update()
    
    def start(self):
        """アニメーション開始"""
        self._timer.start(50)  # 20FPS
    
    def stop(self):
        """アニメーション停止"""
        self._timer.stop()