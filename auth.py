from flask import Blueprint, render_template, redirect, url_for, flash, request
from flask_login import login_user, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from extensions import db
from models import User, UserRole
from forms import RegistrationForm, ResetPasswordForm

auth_bp = Blueprint('auth', __name__)

@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('routes.index'))
        
    form = RegistrationForm()
    
    # Populate teacher choices for student registration
    teachers = User.query.filter_by(role=UserRole.TEACHER).all()
    form.teacher.choices = [(str(t.id), t.username) for t in teachers]
    form.teacher.choices.insert(0, ('', 'Select a teacher'))
    
    if form.validate_on_submit():
        user = User(
            username=form.username.data,
            email=form.email.data,
            role=UserRole(form.role.data)
        )
        user.set_password(form.password.data)
        
        if form.role.data == UserRole.STUDENT.value and form.teacher.data:
            user.teacher_id = int(form.teacher.data)
            
        db.session.add(user)
        db.session.commit()
        
        flash('Your account has been created! You can now log in.', 'success')
        return redirect(url_for('auth.login'))
        
    return render_template('register.html', title='Register', form=form)

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('routes.index'))

    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')

        user = User.query.filter_by(email=email).first()
        if not user or not check_password_hash(user.password_hash, password):  # Changed from password to password_hash
            flash('Invalid email or password.', 'danger')
            return redirect(url_for('auth.login'))

        login_user(user)
        flash('Logged in successfully.', 'success')
        
        # Redirect based on user role
        if user.role == UserRole.TEACHER:  # Changed from string to enum
            return redirect(url_for('routes.teacher_dashboard'))
        else:
            return redirect(url_for('routes.student_dashboard'))

    return render_template('login.html')

@auth_bp.route('/reset_password', methods=['GET', 'POST'])
def reset_password():
    if current_user.is_authenticated:
        return redirect(url_for('routes.index'))
        
    form = ResetPasswordForm()
    if form.validate_on_submit():
        user = User.query.filter_by(email=form.email.data).first()
        if user:
            user.set_password(form.password.data)
            db.session.commit()
            flash('Your password has been reset.', 'success')
            return redirect(url_for('auth.login'))
        flash('Email address not found.', 'danger')
    return render_template('reset_password.html', title='Reset Password', form=form)
