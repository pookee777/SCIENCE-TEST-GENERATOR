from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField, SelectField, TextAreaField, FloatField
from wtforms import BooleanField, IntegerField, HiddenField, RadioField, SelectMultipleField, widgets
from wtforms.validators import DataRequired, Email, EqualTo, ValidationError, Length, Optional, NumberRange
from wtforms.widgets import TextArea
from models import User, QuestionDifficulty, QuestionType, UserRole


# Create a custom MultiCheckbox field
class MultiCheckboxField(SelectMultipleField):
    widget = widgets.ListWidget(prefix_label=False)
    option_widget = widgets.CheckboxInput()


class LoginForm(FlaskForm):
    email = StringField('Email', validators=[DataRequired(), Email()])
    password = PasswordField('Password', validators=[DataRequired()])
    remember_me = BooleanField('Remember Me')
    submit = SubmitField('Log In')


class RegistrationForm(FlaskForm):
    username = StringField('Username', validators=[DataRequired(), Length(min=4, max=64)])
    email = StringField('Email', validators=[DataRequired(), Email()])
    password = PasswordField('Password', validators=[DataRequired(), Length(min=8)])
    confirm_password = PasswordField(
        'Confirm Password', validators=[DataRequired(), EqualTo('password')])
    role = SelectField('Role', choices=[
        (UserRole.STUDENT.value, 'Student'), 
        (UserRole.TEACHER.value, 'Teacher')
    ])
    teacher = SelectField('Teacher (for students only)', choices=[], validators=[Optional()])
    submit = SubmitField('Register')

    def validate_username(self, username):
        user = User.query.filter_by(username=username.data).first()
        if user:
            raise ValidationError('Username is already taken. Please choose a different one.')

    def validate_email(self, email):
        user = User.query.filter_by(email=email.data).first()
        if user:
            raise ValidationError('Email is already registered. Please use a different one.')
            
    def validate_teacher(self, teacher):
        if self.role.data == UserRole.STUDENT.value and not teacher.data:
            raise ValidationError('Students must select a teacher.')


class ResetPasswordForm(FlaskForm):
    email = StringField('Email', validators=[DataRequired(), Email()])
    password = PasswordField('New Password', validators=[DataRequired(), Length(min=8)])
    confirm_password = PasswordField(
        'Confirm Password', validators=[DataRequired(), EqualTo('password')])
    submit = SubmitField('Reset Password')


class QuestionForm(FlaskForm):
    text = TextAreaField('Question Text', validators=[DataRequired()], widget=TextArea())
    chapter_id = SelectField('Chapter', coerce=int, validators=[DataRequired()])
    difficulty = SelectField('Difficulty', choices=[
        (diff.value, diff.name.capitalize()) for diff in QuestionDifficulty
    ], validators=[DataRequired()])
    question_type = SelectField('Question Type', choices=[
        (qtype.value, qtype.name.replace('_', ' ').capitalize()) for qtype in QuestionType
    ], validators=[DataRequired()])
    marks = IntegerField('Marks', validators=[DataRequired()])
    option_a = StringField('Option A')
    option_b = StringField('Option B')
    option_c = StringField('Option C')
    option_d = StringField('Option D')
    correct_answer = TextAreaField('Correct Answer', validators=[DataRequired()], widget=TextArea())
    solution = TextAreaField('Detailed Solution', widget=TextArea())
    submit = SubmitField('Save Question')


class CreateTestForm(FlaskForm):
    title = StringField('Test Title', validators=[DataRequired()])
    description = TextAreaField('Description', widget=TextArea())
    duration_minutes = IntegerField('Duration (minutes)', validators=[DataRequired()])
    is_public = BooleanField('Assign to all students')
    submit = SubmitField('Create Test')


class StudentGenerateTestForm(FlaskForm):
    title = StringField('Test Title', validators=[DataRequired()])
    chapters = MultiCheckboxField('Chapters', coerce=int, validators=[Optional()])
    difficulty = SelectField('Difficulty', choices=[
        ('all', 'All Difficulties'),
        (QuestionDifficulty.EASY.value, 'Easy'),
        (QuestionDifficulty.MEDIUM.value, 'Medium'),
        (QuestionDifficulty.HARD.value, 'Hard')
    ], validators=[DataRequired()])
    question_type = SelectField('Question Type', choices=[
        ('all', 'All Types'),
        (QuestionType.MULTIPLE_CHOICE.value, 'Multiple Choice'),
        (QuestionType.TRUE_FALSE.value, 'True/False'),
        (QuestionType.NUMERICAL.value, 'Numerical'),
        (QuestionType.DESCRIPTIVE.value, 'Descriptive')
    ], validators=[DataRequired()])
    num_questions = IntegerField('Number of Questions', validators=[DataRequired()])
    duration_minutes = IntegerField('Duration (minutes)', validators=[DataRequired()])
    submit = SubmitField('Generate Test')


class PersonalizedTestForm(FlaskForm):
    title = StringField('Test Title', validators=[DataRequired()])
    chapters = MultiCheckboxField('Chapters (Optional)', coerce=int, validators=[Optional()])
    num_questions = IntegerField('Number of Questions', validators=[DataRequired(), NumberRange(min=3, max=30)])
    duration_minutes = IntegerField('Duration (minutes)', validators=[DataRequired(), NumberRange(min=5, max=180)])
    submit = SubmitField('Generate Personalized Test')


class AnswerForm(FlaskForm):
    question_id = HiddenField('Question ID')
    test_result_id = HiddenField('Test Result ID')
    
    # For multiple choice questions
    multiple_choice = RadioField('Answer', choices=[])
    
    # For true/false questions
    true_false = RadioField('Answer', choices=[('True', 'True'), ('False', 'False')])
    
    # For numerical and descriptive questions
    text_answer = TextAreaField('Answer', widget=TextArea())
    
    # To determine which field to show
    question_type = HiddenField('Question Type')
