from datetime import datetime, timedelta
import enum
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import ForeignKey, Enum
from sqlalchemy.orm import relationship
from extensions import db, login_manager
from time import time
import jwt
from flask import current_app


class UserRole(str, enum.Enum):
    TEACHER = "teacher"
    STUDENT = "student"


class QuestionDifficulty(enum.Enum):
    EASY = "easy"
    MEDIUM = "medium"
    HARD = "hard"


class QuestionType(enum.Enum):
    MULTIPLE_CHOICE = "multiple_choice"
    TRUE_FALSE = "true_false"
    NUMERICAL = "numerical"
    DESCRIPTIVE = "descriptive"


class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(128))
    role = db.Column(db.Enum(UserRole), nullable=False)
    teacher_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Simplified relationships
    students = db.relationship(
        "User",
        backref=db.backref("teacher", remote_side=[id]),
        lazy="select",  # Changed from dynamic
        primaryjoin="User.teacher_id==User.id"
    )
    
    created_tests = db.relationship(
        "Test",
        backref="creator",
        lazy="select",  # Changed from dynamic
        foreign_keys="Test.creator_id"
    )
    
    test_results = relationship("TestResult", backref="student", lazy=True)
    
    # Password reset token fields
    reset_token = db.Column(db.String(100), nullable=True)
    reset_token_expiry = db.Column(db.DateTime, nullable=True)

    @property
    def is_teacher(self):
        return self.role == UserRole.TEACHER

    @property
    def is_student(self):
        return self.role == UserRole.STUDENT

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)
    
    def generate_reset_token(self, expires_in=3600):
        import secrets
        self.reset_token = secrets.token_urlsafe(32)
        self.reset_token_expiry = datetime.utcnow() + timedelta(seconds=expires_in)
        return self.reset_token
    
    def get_reset_password_token(self, expires_in=600):
        """Generate a password reset token that expires in 10 minutes"""
        return jwt.encode(
            {
                'reset_password': self.id,
                'exp': time() + expires_in
            },
            current_app.config['SECRET_KEY'],
            algorithm='HS256'
        )

    @staticmethod
    def verify_reset_token(token):
        """Verify the password reset token"""
        try:
            id = jwt.decode(
                token,
                current_app.config['SECRET_KEY'],
                algorithms=['HS256']
            )['reset_password']
        except:
            return None
        return User.query.get(id)
    
    def clear_reset_token(self):
        self.reset_token = None
        self.reset_token_expiry = None


class Chapter(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    description = db.Column(db.Text, nullable=True)
    
    # Relationships
    questions = relationship("Question", backref="chapter", lazy=True)


class Question(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    text = db.Column(db.Text, nullable=False)
    chapter_id = db.Column(db.Integer, ForeignKey("chapter.id"), nullable=False)
    difficulty = db.Column(Enum(QuestionDifficulty), nullable=False)
    question_type = db.Column(Enum(QuestionType), nullable=False)
    marks = db.Column(db.Integer, nullable=False)
    created_by = db.Column(db.Integer, ForeignKey("user.id"), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Multiple choice options (applicable for MULTIPLE_CHOICE type)
    option_a = db.Column(db.Text, nullable=True)
    option_b = db.Column(db.Text, nullable=True)
    option_c = db.Column(db.Text, nullable=True)
    option_d = db.Column(db.Text, nullable=True)
    
    # Answer for all question types
    correct_answer = db.Column(db.Text, nullable=False)
    
    # Detailed solution
    solution = db.Column(db.Text, nullable=True)
    
    # Image path for question
    image_path = db.Column(db.String(255), nullable=True)
    
    # Relationships
    creator = relationship("User", foreign_keys=[created_by])
    test_questions = relationship("TestQuestion", backref="question", lazy=True)


class Test(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    description = db.Column(db.Text, nullable=True)
    duration_minutes = db.Column(db.Integer, nullable=False)
    total_marks = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    creator_id = db.Column(db.Integer, ForeignKey("user.id"), nullable=False)
    is_public = db.Column(db.Boolean, default=False)  # True if assigned to all students
    
    # Relationships
    questions = relationship("TestQuestion", backref="test", lazy=True, cascade="all, delete-orphan")
    results = relationship("TestResult", backref="test", lazy=True)


class TestQuestion(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    test_id = db.Column(db.Integer, ForeignKey("test.id"), nullable=False)
    question_id = db.Column(db.Integer, ForeignKey("question.id"), nullable=False)
    order = db.Column(db.Integer, nullable=False)


class TestResult(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    test_id = db.Column(db.Integer, ForeignKey("test.id"), nullable=False)
    student_id = db.Column(db.Integer, ForeignKey("user.id"), nullable=False)
    start_time = db.Column(db.DateTime, default=datetime.utcnow)
    end_time = db.Column(db.DateTime, nullable=True)
    total_score = db.Column(db.Float, nullable=True)
    completed = db.Column(db.Boolean, default=False)
    
    # Relationships
    answers = relationship("QuestionAnswer", backref="test_result", lazy=True, cascade="all, delete-orphan")


class QuestionAnswer(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    test_result_id = db.Column(db.Integer, ForeignKey("test_result.id"), nullable=False)
    question_id = db.Column(db.Integer, ForeignKey("question.id"), nullable=False)
    student_answer = db.Column(db.Text, nullable=True)
    is_correct = db.Column(db.Boolean, nullable=True)
    score = db.Column(db.Float, nullable=True)
    
    # Relationship
    question = relationship("Question")


@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))
