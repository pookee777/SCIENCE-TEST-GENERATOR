from datetime import datetime, timedelta
import random
from urllib.parse import urlsplit
from flask import Blueprint, render_template, redirect, url_for, flash, request, jsonify, current_app, abort
from flask_login import login_user, logout_user, login_required, current_user
from extensions import db

def create_routes():
    routes_bp = Blueprint('routes', __name__)
    
    from models import (User, Question, Chapter, Test, TestQuestion, TestResult, 
                       QuestionAnswer, UserRole, QuestionDifficulty, QuestionType)
    from forms import (LoginForm, RegistrationForm, ResetPasswordForm,
                      QuestionForm, CreateTestForm, StudentGenerateTestForm,
                      PersonalizedTestForm, AnswerForm)
    from utils import format_duration, calculate_grade, utc_to_local
    from recommendation import recommender

    @routes_bp.route('/')
    def index():
        return render_template('index.html')

    # Add a context processor to make variables available to all templates
    @routes_bp.app_context_processor
    def inject_now():
        return {'now': datetime.utcnow()}

    # Debug route for static files
    @routes_bp.route('/debug/static')
    def debug_static():
        return render_template('debug_static.html')

    # Debug route for database tables
    @routes_bp.route('/debug/db')
    def debug_db():
        tables = db.inspect(db.engine).get_table_names()
        return jsonify({"tables": tables})

    # Error handler for 404
    @routes_bp.errorhandler(404)
    def page_not_found(e):
        return "Page not found", 404

    @routes_bp.route('/register', methods=['GET', 'POST'])
    def register():
        if current_user.is_authenticated:
            if current_user.role == UserRole.TEACHER:
                return redirect(url_for('routes.teacher_dashboard'))
            else:
                return redirect(url_for('routes.student_dashboard'))
                
        form = RegistrationForm()
        
        # Populate teacher choices for student registration
        teachers = User.query.filter_by(role=UserRole.TEACHER).all()
        form.teacher.choices = [(str(t.id), t.username) for t in teachers]
        form.teacher.choices.insert(0, ('', 'Select a teacher'))
        
        if form.validate_on_submit():
            # Validate teacher selection for students
            if form.role.data == UserRole.STUDENT.value and not form.teacher.data:
                flash('Students must select a teacher.', 'danger')
                return render_template('register.html', title='Register', form=form)

            user = User(
                username=form.username.data,
                email=form.email.data,
                role=UserRole(form.role.data)
            )
            user.set_password(form.password.data)
            
            # Always set teacher_id for students
            if form.role.data == UserRole.STUDENT.value:
                user.teacher_id = int(form.teacher.data)
            
            db.session.add(user)
            try:
                db.session.commit()
                flash('Your account has been created! You can now log in.', 'success')
                return redirect(url_for('auth.login'))
            except Exception as e:
                db.session.rollback()
                flash('Registration failed. Please try again.', 'danger')
                return render_template('register.html', title='Register', form=form)
            
        return render_template('register.html', title='Register', form=form)


    @routes_bp.route('/logout')
    @login_required
    def logout():
        logout_user()
        flash('You have been logged out.', 'success')
        return redirect(url_for('routes.index'))


    @routes_bp.route('/reset_password_request', methods=['GET', 'POST'])
    def reset_password_request():
        if current_user.is_authenticated:
            return redirect(url_for('routes.index'))
            
        form = ResetPasswordForm()  # Changed from ResetPasswordRequestForm
        if form.validate_on_submit():
            user = User.query.filter_by(email=form.email.data).first()
            if user:
                user.set_password(form.password.data)  # Directly reset password
                db.session.commit()
                flash('Your password has been reset.', 'success')
                return redirect(url_for('auth.login'))
            flash('Email address not found.', 'danger')
        return render_template('reset_password.html', title='Reset Password', form=form)


    @routes_bp.route('/reset_password/<token>', methods=['GET', 'POST'])
    def reset_password(token):
        if current_user.is_authenticated:
            return redirect(url_for('routes.index'))
            
        # Find user with this token
        user = User.query.filter_by(reset_token=token).first()
        if not user or not user.verify_reset_token(token):
            flash('Invalid or expired reset token', 'danger')
            return redirect(url_for('routes.reset_password_request'))
        
        form = ResetPasswordForm()
        if form.validate_on_submit():
            user.set_password(form.password.data)
            user.clear_reset_token()
            db.session.commit()
            flash('Your password has been reset.', 'success')
            return redirect(url_for('auth.login'))
            
        return render_template('reset_password.html', title='Reset Password', form=form)


    # Teacher routes
    @routes_bp.route('/teacher/dashboard')
    @login_required
    def teacher_dashboard():
        if current_user.role != UserRole.TEACHER:
            flash("Access denied. Teacher permissions required.", "danger")
            return redirect(url_for('routes.index'))
            
        # Get student performance data
        students = User.query.filter_by(teacher_id=current_user.id).all()
        student_ids = [student.id for student in students]
        
        # Get test data
        recent_tests = Test.query.filter_by(creator_id=current_user.id).order_by(Test.created_at.desc()).limit(5).all()
        
        # Get metrics for dashboard
        total_students = len(students)
        total_tests = Test.query.filter_by(creator_id=current_user.id).count()
        total_questions = Question.query.filter_by(created_by=current_user.id).count()
        
        # Get aggregated student performance data
        student_performance = []
        if student_ids:
            for student in students:
                results = TestResult.query.filter_by(student_id=student.id, completed=True).all()
                if results:
                    # Calculate average of percentages
                    percentages = [(result.total_score / Test.query.get(result.test_id).total_marks) for result in results]
                    avg_percentage = sum(percentages) / len(percentages)
                    tests_taken = len(results)
                    student_performance.append({
                        'id': student.id,
                        'name': student.username,
                        'tests_taken': tests_taken,
                        'avg_score': avg_percentage  # This is now already a percentage (0-1)
                    })
                else:
                    student_performance.append({
                        'id': student.id,
                        'name': student.username,
                        'tests_taken': 0,
                        'avg_score': 0
                    })
        
        return render_template('teacher/dashboard.html', 
                              title='Teacher Dashboard',
                              students=students,
                              recent_tests=recent_tests,
                              total_students=total_students,
                              total_tests=total_tests,
                              total_questions=total_questions,
                              student_performance=student_performance)


    @routes_bp.route('/teacher/students')
    @login_required
    def teacher_students():
        if current_user.role != UserRole.TEACHER:
            abort(403)

        # Get all users with this teacher_id
        students = User.query.filter_by(teacher_id=current_user.id).all()
        # Debug: print all users found with this teacher_id
        print("DEBUG: Users with teacher_id", current_user.id)
        for s in students:
            print(f"User: {s.username}, role: {s.role}, type: {type(s.role)}")

        # Filter for students robustly
        students = [
            s for s in students
            if (
                getattr(s, "role", None) == UserRole.STUDENT or
                getattr(s, "role", None) == UserRole.STUDENT.value or
                str(getattr(s, "role", None)).upper() == "STUDENT"
            )
        ]

        # Debug: print after filtering
        print("DEBUG: Students after filtering")
        for s in students:
            print(f"Student: {s.username}, role: {s.role}")

        # Format student data for JSON serialization
        student_data = []
        for student in students:
            completed_tests = TestResult.query.filter_by(student_id=student.id, completed=True).count()
            
            # Calculate average score
            results = TestResult.query.filter_by(student_id=student.id, completed=True).all()
            if results:
                total_percentage = sum(
                    (r.total_score / r.test.total_marks * 100) 
                    for r in results 
                    if r.test.total_marks > 0
                )
                avg_score = total_percentage / len(results) if results else 0
            else:
                avg_score = 0

            student_data.append({
                'id': student.id,
                'username': student.username,
                'email': student.email,
                'tests_taken': completed_tests,
                'avg_score': round(avg_score, 1)
            })

        # Debug: print student_data before rendering
        print("DEBUG: student_data to render:", student_data)

        return render_template('teacher/students.html', student_data=student_data)


    @routes_bp.route('/teacher/manage_questions', methods=['GET', 'POST'])
    @login_required
    def manage_questions():
        if current_user.role != UserRole.TEACHER:
            flash("Access denied. Teacher permissions required.", "danger")
            return redirect(url_for('routes.index'))
            
        form = QuestionForm()
        
        # Populate chapter choices
        chapters = Chapter.query.all()
        form.chapter_id.choices = [(c.id, c.name) for c in chapters]
        
        if form.validate_on_submit():
            question = Question(
                text=form.text.data,
                chapter_id=form.chapter_id.data,
                difficulty=QuestionDifficulty(form.difficulty.data),
                question_type=QuestionType(form.question_type.data),
                marks=form.marks.data,
                created_by=current_user.id,
                option_a=form.option_a.data,
                option_b=form.option_b.data,
                option_c=form.option_c.data,
                option_d=form.option_d.data,
                correct_answer=form.correct_answer.data,
                solution=form.solution.data
            )
            db.session.add(question)
            db.session.commit()
            flash('Question added successfully!', 'success')
            return redirect(url_for('routes.manage_questions'))
        
        # Get all questions including system questions
        questions = Question.query.order_by(Question.created_at.desc()).all()
        
        return render_template('teacher/manage_questions.html',
                             title='Manage Questions',
                             form=form,
                             questions=questions,
                             chapters={c.id: c.name for c in chapters})


    @routes_bp.route('/teacher/edit_question/<int:question_id>', methods=['GET', 'POST'])
    @login_required
    def edit_question(question_id):
        if current_user.role != UserRole.TEACHER:
            flash("Access denied. Teacher permissions required.", "danger")
            return redirect(url_for('routes.index'))
            
        question = Question.query.get_or_404(question_id)
        
        # Only allow editing if the teacher created the question or it's a system question
        if question.created_by is not None and question.created_by != current_user.id:
            flash("You can only edit your own questions.", "danger")
            return redirect(url_for('routes.manage_questions'))
        
        form = QuestionForm(obj=question)
        
        # Populate chapter choices
        chapters = Chapter.query.all()
        form.chapter_id.choices = [(c.id, c.name) for c in chapters]
        
        if form.validate_on_submit():
            question.text = form.text.data
            question.chapter_id = form.chapter_id.data
            question.difficulty = QuestionDifficulty(form.difficulty.data)
            question.question_type = QuestionType(form.question_type.data)
            question.marks = form.marks.data
            question.option_a = form.option_a.data
            question.option_b = form.option_b.data
            question.option_c = form.option_c.data
            question.option_d = form.option_d.data
            question.correct_answer = form.correct_answer.data
            question.solution = form.solution.data
            
            db.session.commit()
            flash('Question updated successfully!', 'success')
            return redirect(url_for('routes.manage_questions'))
            
        return render_template('teacher/manage_questions.html',
                             title='Edit Question',
                             form=form,
                             questions=Question.query.all(),
                             chapters={c.id: c.name for c in chapters},
                             edit_mode=True,
                             question=question)


    @routes_bp.route('/teacher/delete_question/<int:question_id>', methods=['POST'])
    @login_required
    def delete_question(question_id):
        if current_user.role != UserRole.TEACHER:
            return jsonify({'success': False, 'message': 'Access denied'}), 403
            
        question = Question.query.get_or_404(question_id)
        
        # Only allow deleting if the teacher created the question
        if question.created_by is not None and question.created_by != current_user.id:
            return jsonify({'success': False, 'message': 'You can only delete your own questions'}), 403
        
        db.session.delete(question)
        db.session.commit()
        
        return jsonify({'success': True})


    @routes_bp.route('/teacher/create_test', methods=['GET', 'POST'])
    @login_required
    def create_test():
        if current_user.role != UserRole.TEACHER:
            flash("Access denied. Teacher permissions required.", "danger")
            return redirect(url_for('routes.index'))
            
        form = CreateTestForm()
        
        if form.validate_on_submit():
            # Create a new test
            test = Test(
                title=form.title.data,
                description=form.description.data,
                duration_minutes=form.duration_minutes.data,
                creator_id=current_user.id,
                is_public=form.is_public.data,
                total_marks=0  # Will be updated once questions are added
            )
            db.session.add(test)
            db.session.flush()  # Get the test ID
            
            # Now redirect to add questions to the test
            db.session.commit()
            return redirect(url_for('routes.add_questions_to_test', test_id=test.id))
        
        return render_template('teacher/create_test.html', title='Create Test', form=form)


    @routes_bp.route('/teacher/test/<int:test_id>/add_questions', methods=['GET', 'POST'])
    @login_required
    def add_questions_to_test(test_id):
        if current_user.role != UserRole.TEACHER:
            flash("Access denied. Teacher permissions required.", "danger")
            return redirect(url_for('routes.index'))
            
        test = Test.query.get_or_404(test_id)
        
        # Ensure the teacher owns this test
        if test.creator_id != current_user.id:
            flash("You can only modify your own tests.", "danger")
            return redirect(url_for('routes.teacher_dashboard'))
        
        # Get all available questions
        chapters = Chapter.query.all()
        questions = Question.query.all()
        
        # Get currently added questions
        test_questions = TestQuestion.query.filter_by(test_id=test_id).order_by(TestQuestion.order).all()
        added_question_ids = [tq.question_id for tq in test_questions]
        
        if request.method == 'POST':
            # Process form submission to add or remove questions
            selected_questions = request.form.getlist('selected_questions')
            
            # First, remove all existing questions
            TestQuestion.query.filter_by(test_id=test_id).delete()
            
            # Add new questions in order
            total_marks = 0
            for i, q_id in enumerate(selected_questions):
                question = Question.query.get(int(q_id))
                if question:
                    test_question = TestQuestion(
                        test_id=test_id,
                        question_id=question.id,
                        order=i+1
                    )
                    db.session.add(test_question)
                    total_marks += question.marks
            
            # Update test total marks
            test.total_marks = total_marks
            db.session.commit()
            
            flash("Test questions have been updated.", "success")
            return redirect(url_for('routes.teacher_dashboard'))
        
        return render_template('teacher/create_test.html', 
                              title='Add Questions to Test',
                              test=test,
                              chapters=chapters,
                              questions=questions,
                              added_question_ids=added_question_ids,
                              edit_mode=True)


    @routes_bp.route('/teacher/test/<int:test_id>')
    @login_required
    def view_test(test_id):
        if current_user.role != UserRole.TEACHER:
            flash("Access denied. Teacher permissions required.", "danger")
            return redirect(url_for('routes.index'))
            
        test = Test.query.get_or_404(test_id)
        
        # Ensure the teacher owns this test
        if test.creator_id != current_user.id:
            flash("You can only view your own tests.", "danger")
            return redirect(url_for('routes.teacher_dashboard'))
        
        # Get questions in order
        test_questions = db.session.query(Question, TestQuestion).\
            join(TestQuestion, Question.id == TestQuestion.question_id).\
            filter(TestQuestion.test_id == test_id).\
            order_by(TestQuestion.order).all()
        
        # Get test results if any
        results = TestResult.query.filter_by(test_id=test_id, completed=True).all()
        
        return render_template('teacher/view_test.html',
                              title=f'Test: {test.title}',
                              test=test,
                              test_questions=test_questions,
                              results=results)


    # Student routes
    @routes_bp.route('/student/dashboard')
    @login_required
    def student_dashboard():
        if current_user.role != UserRole.STUDENT:
            flash("Access denied. Student permissions required.", "danger")
            return redirect(url_for('routes.index'))
        
        # Get assigned tests from teacher
        assigned_tests = Test.query.filter(
            (Test.creator_id == current_user.teacher_id) & 
            (Test.is_public == True)
        ).all()
        
        # Get student's completed tests
        completed_tests = TestResult.query.filter_by(
            student_id=current_user.id,
            completed=True
        ).order_by(TestResult.end_time.desc()).all()
        
        # Get in-progress tests
        in_progress_tests = TestResult.query.filter_by(
            student_id=current_user.id,
            completed=False
        ).all()
        
        # Calculate performance metrics
        total_tests_taken = len(completed_tests)
        avg_score = 0

        if total_tests_taken > 0:
            percentages = []
            for result in completed_tests:
                test = Test.query.get(result.test_id)
                if test.total_marks > 0:
                    percentage = (result.total_score / test.total_marks) * 100
                    percentages.append(percentage)
            if percentages:
                avg_score = sum(percentages) / len(percentages)
        
        # Get assigned tests that haven't been started
        taken_test_ids = [result.test_id for result in completed_tests + in_progress_tests]
        new_assigned_tests = [test for test in assigned_tests if test.id not in taken_test_ids]
        
        return render_template('student/dashboard.html',
                              title='Student Dashboard',
                              assigned_tests=new_assigned_tests,
                              completed_tests=completed_tests,
                              in_progress_tests=in_progress_tests,
                              total_tests_taken=total_tests_taken,
                              avg_score=round(avg_score, 2))


    @routes_bp.route('/student/create_test', methods=['GET', 'POST'])
    @login_required
    def student_create_test():
        form = StudentGenerateTestForm()
        
        # Populate chapter choices with checkboxes
        chapters = Chapter.query.all()
        form.chapters.choices = [(c.id, c.name) for c in chapters]
        
        if form.validate_on_submit():
            if not form.chapters.data:
                flash('Please select at least one chapter.', 'danger')
                return redirect(url_for('routes.student_create_test'))
            
            # Get questions based on criteria
            query = Question.query
            
            # Filter by selected chapters
            if form.chapters.data:
                query = query.filter(Question.chapter_id.in_(form.chapters.data))
            
            # Filter by difficulty if not 'all'
            if form.difficulty.data != 'all':
                query = query.filter_by(difficulty=QuestionDifficulty(form.difficulty.data))
            
            # Filter by question type if not 'all'
            if form.question_type.data != 'all':
                query = query.filter_by(question_type=QuestionType(form.question_type.data))
            
            # Get all matching questions
            available_questions = query.all()
            
            # Ensure we have enough questions
            if len(available_questions) < form.num_questions.data:
                flash(f"Not enough questions available with these criteria. Found {len(available_questions)}.", "danger")
                return redirect(url_for('routes.student_create_test'))
            
            # Randomly select questions
            selected_questions = random.sample(available_questions, form.num_questions.data)
            
            # Create a new test
            test = Test(
                title=form.title.data,
                description=f"Custom test created by {current_user.username}",
                duration_minutes=form.duration_minutes.data,
                creator_id=current_user.id,  # Student is technically the creator
                is_public=False,  # This is a personal test
                total_marks=sum(q.marks for q in selected_questions)
            )
            db.session.add(test)
            db.session.flush()  # Get the test ID
            
            # Add questions to test
            for i, question in enumerate(selected_questions):
                test_question = TestQuestion(
                    test_id=test.id,
                    question_id=question.id,
                    order=i+1
                )
                db.session.add(test_question)
            
            # Create test result (starts the test)
            test_result = TestResult(
                test_id=test.id,
                student_id=current_user.id,
                start_time=datetime.utcnow(),
                completed=False
            )
            db.session.add(test_result)
            db.session.commit()
            
            flash(f"Test '{form.title.data}' created and started!", "success")
            return redirect(url_for('routes.take_test', result_id=test_result.id))
        
        return render_template('student/create_test.html',
                              title='Create Custom Test',
                              form=form)


    @routes_bp.route('/student/personalized_test', methods=['GET', 'POST'])
    @login_required
    def personalized_test():
        if current_user.role != UserRole.STUDENT:
            flash("Access denied. Student permissions required.", "danger")
            return redirect(url_for('routes.index'))
        
        form = PersonalizedTestForm()
        
        # Populate chapter choices with checkboxes
        chapters = Chapter.query.all()
        form.chapters.choices = [(c.id, c.name) for c in chapters]
        
        if form.validate_on_submit():
            try:
                # Use the recommender to get personalized questions
                chapter_ids = form.chapters.data if form.chapters.data else None
                recommended_questions = recommender.recommend_questions(
                    student_id=current_user.id, 
                    chapter_ids=chapter_ids,
                    num_questions=form.num_questions.data
                )
                
                # Ensure we have enough questions
                if len(recommended_questions) < form.num_questions.data:
                    flash(f"Not enough questions available for personalized recommendations. Found {len(recommended_questions)}.", "warning")
                    # If we don't have enough recommended questions, fall back to a mix of questions
                    available_questions = Question.query
                    if chapter_ids:
                        available_questions = available_questions.filter(Question.chapter_id.in_(chapter_ids))
                    additional_needed = form.num_questions.data - len(recommended_questions)
                    # Exclude questions already recommended
                    recommended_ids = [q.id for q in recommended_questions]
                    additional_questions = available_questions.filter(~Question.id.in_(recommended_ids)).limit(additional_needed).all()
                    recommended_questions.extend(additional_questions)
                
                # Create a new test
                test = Test(
                    title=form.title.data,
                    description=f"Personalized test generated for {current_user.username}",
                    duration_minutes=form.duration_minutes.data,
                    creator_id=current_user.id,
                    is_public=False,  # Personal test
                    total_marks=sum(q.marks for q in recommended_questions)
                )
                db.session.add(test)
                db.session.flush()
                
                # Add questions to test
                for i, question in enumerate(recommended_questions):
                    test_question = TestQuestion(
                        test_id=test.id,
                        question_id=question.id,
                        order=i+1
                    )
                    db.session.add(test_question)
                
                # Create test result (starts the test)
                test_result = TestResult(
                    test_id=test.id,
                    student_id=current_user.id,
                    start_time=datetime.utcnow(),
                    completed=False
                )
                db.session.add(test_result)
                db.session.commit()
                
                flash(f"Personalized test '{form.title.data}' created based on your performance history!", "success")
                return redirect(url_for('routes.take_test', result_id=test_result.id))
                
            except Exception as e:
                # Handle errors gracefully
                flash(f"Could not generate personalized test: {str(e)}", "danger")
                return redirect(url_for('routes.personalized_test'))
        
        return render_template('student/personalized_test.html',
                              title='Generate Personalized Test',
                              form=form)


    @routes_bp.route('/student/start_test/<int:test_id>', methods=['POST'])
    @login_required
    def start_test(test_id):
        if current_user.role != UserRole.STUDENT:
            flash("Access denied. Student permissions required.", "danger")
            return redirect(url_for('routes.index'))
        
        test = Test.query.get_or_404(test_id)
        
        # Check if test is already in progress
        existing_result = TestResult.query.filter_by(
            test_id=test_id,
            student_id=current_user.id,
            completed=False
        ).first()
        
        if existing_result:
            return redirect(url_for('routes.take_test', result_id=existing_result.id))
        
        # Check if the test has already been completed
        completed_result = TestResult.query.filter_by(
            test_id=test_id,
            student_id=current_user.id,
            completed=True
        ).first()
        
        if completed_result:
            flash("You have already completed this test.", "info")
            return redirect(url_for('routes.test_results', result_id=completed_result.id))
        
        # Create a new test result
        test_result = TestResult(
            test_id=test_id,
            student_id=current_user.id,
            start_time=datetime.utcnow(),
            completed=False
        )
        db.session.add(test_result)
        db.session.commit()
        
        return redirect(url_for('routes.take_test', result_id=test_result.id))


    @routes_bp.route('/student/take_test/<int:result_id>', methods=['GET', 'POST'])
    @login_required
    def take_test(result_id):
        test_result = TestResult.query.get_or_404(result_id)
        
        # Convert start time to local timezone
        local_start_time = utc_to_local(test_result.start_time)
        
        # Calculate remaining time in seconds
        elapsed_time = datetime.utcnow() - test_result.start_time
        total_duration = timedelta(minutes=test_result.test.duration_minutes)
        remaining_seconds = max(0, int((total_duration - elapsed_time).total_seconds()))
        
        # Ensure the student owns this test result
        if test_result.student_id != current_user.id:
            flash("Access denied. This is not your test.", "danger")
            return redirect(url_for('routes.student_dashboard'))
        
        # If test is already completed, show results
        if test_result.completed:
            return redirect(url_for('routes.test_results', result_id=result_id))
        
        test = Test.query.get(test_result.test_id)
        
        # Check if time is up
        now = datetime.utcnow()
        end_time = test_result.start_time + timedelta(minutes=test.duration_minutes)
        time_remaining = max(0, int((end_time - now).total_seconds()))
        
        if time_remaining <= 0 and not test_result.completed:
            # Time's up, auto-submit
            return auto_submit_test(result_id)
        
        # Get questions in order
        test_questions = db.session.query(Question, TestQuestion).\
            join(TestQuestion, Question.id == TestQuestion.question_id).\
            filter(TestQuestion.test_id == test.id).\
            order_by(TestQuestion.order).all()
        
        # Create a form for each question
        forms = []
        for question, test_question in test_questions:
            form = AnswerForm(prefix=f"q{question.id}")
            form.question_id.data = question.id
            form.test_result_id.data = test_result.id
            form.question_type.data = question.question_type.value
            
            # Set up multiple choice options if applicable
            if question.question_type == QuestionType.MULTIPLE_CHOICE:
                form.multiple_choice.choices = [
                    ('A', question.option_a),
                    ('B', question.option_b),
                    ('C', question.option_c),
                    ('D', question.option_d)
                ]
            
            # Get existing answer if any
            existing_answer = QuestionAnswer.query.filter_by(
                test_result_id=test_result.id,
                question_id=question.id
            ).first()
            
            if existing_answer:
                if question.question_type == QuestionType.MULTIPLE_CHOICE:
                    form.multiple_choice.data = existing_answer.student_answer
                elif question.question_type == QuestionType.TRUE_FALSE:
                    form.true_false.data = existing_answer.student_answer
                else:
                    form.text_answer.data = existing_answer.student_answer
            
            forms.append((question, form))
        
        if request.method == 'POST':
            # Handle form submission
            if 'save_progress' in request.form:
                # Save current progress
                for question, form in forms:
                    if form.validate():
                        # Get the appropriate answer field based on question type
                        if question.question_type == QuestionType.MULTIPLE_CHOICE:
                            answer = form.multiple_choice.data
                        elif question.question_type == QuestionType.TRUE_FALSE:
                            answer = form.true_false.data
                        else:
                            answer = form.text_answer.data
                        
                        # Update or create answer
                        existing_answer = QuestionAnswer.query.filter_by(
                            test_result_id=test_result.id,
                            question_id=question.id
                        ).first()
                        
                        if existing_answer:
                            existing_answer.student_answer = answer
                        else:
                            new_answer = QuestionAnswer(
                                test_result_id=test_result.id,
                                question_id=question.id,
                                student_answer=answer
                            )
                            db.session.add(new_answer)
                
                db.session.commit()
                flash("Progress saved!", "success")
                return redirect(url_for('routes.take_test', result_id=result_id))
                
            elif 'submit_test' in request.form:
                # First save all answers from the form
                for question, form in forms:
                    # Get the appropriate answer field based on question type
                    if question.question_type == QuestionType.MULTIPLE_CHOICE:
                        answer = form.multiple_choice.data
                    elif question.question_type == QuestionType.TRUE_FALSE:
                        answer = form.true_false.data
                    else:
                        answer = form.text_answer.data
                    
                    # Update or create answer if answer has a value
                    if answer:
                        existing_answer = QuestionAnswer.query.filter_by(
                            test_result_id=test_result.id,
                            question_id=question.id
                        ).first()
                        
                        if existing_answer:
                            existing_answer.student_answer = answer
                        else:
                            new_answer = QuestionAnswer(
                                test_result_id=test_result.id,
                                question_id=question.id,
                                student_answer=answer
                            )
                            db.session.add(new_answer)
                
                db.session.commit()
                
                # Then submit the test
                return submit_test(result_id)
        
        # Get answered/unanswered counts
        answered = QuestionAnswer.query.filter_by(
            test_result_id=test_result.id
        ).filter(QuestionAnswer.student_answer != None).count()

        total_questions = len(test_questions)
        unanswered = total_questions - answered

        return render_template('student/take_test.html',
                              title=f'Taking: {test.title}',
                              test=test,
                              test_result=test_result,
                              forms=forms,
                              time_remaining=remaining_seconds,
                              answered=answered,
                              unanswered=unanswered,
                              local_start_time=local_start_time)


    def submit_test(result_id):
        test_result = TestResult.query.get_or_404(result_id)
        
        # Ensure the student owns this test result
        if test_result.student_id != current_user.id:
            flash("Access denied. This is not your test.", "danger")
            return redirect(url_for('routes.student_dashboard'))
        
        # Get the test
        test = Test.query.get(test_result.test_id)
        
        # Get all questions for this test
        test_questions = TestQuestion.query.filter_by(test_id=test.id).all()
        question_ids = [tq.question_id for tq in test_questions]
        questions = Question.query.filter(Question.id.in_(question_ids)).all()
        questions_dict = {q.id: q for q in questions}
        
        # Get all answers
        answers = QuestionAnswer.query.filter_by(test_result_id=test_result.id).all()
        answers_dict = {a.question_id: a for a in answers}
        
        # Grade each question
        total_score = 0
        for question_id in question_ids:
            question = questions_dict.get(question_id)
            answer = answers_dict.get(question_id)
            
            if question and answer:
                # Grade based on question type
                if question.question_type in [QuestionType.MULTIPLE_CHOICE, QuestionType.TRUE_FALSE]:
                    # For multiple choice and true/false, exact match required
                    is_correct = answer.student_answer == question.correct_answer
                    score = question.marks if is_correct else 0
                elif question.question_type == QuestionType.NUMERICAL:
                    # For numerical, allow a small margin of error
                    try:
                        student_value = float(answer.student_answer.strip())
                        correct_value = float(question.correct_answer.strip())
                        is_correct = abs(student_value - correct_value) < 0.001
                        score = question.marks if is_correct else 0
                    except (ValueError, TypeError):
                        is_correct = False
                        score = 0
                else:
                    # For descriptive questions, can't auto-grade
                    # In a real system, this would be graded by the teacher
                    # For now, give partial credit if answer is provided
                    is_correct = None
                    score = question.marks * 0.5 if answer.student_answer else 0
                
                answer.is_correct = is_correct
                answer.score = score
                total_score += score
            elif question:
                # Question not answered
                new_answer = QuestionAnswer(
                    test_result_id=test_result.id,
                    question_id=question.id,
                    student_answer="",
                    is_correct=False,
                    score=0
                )
                db.session.add(new_answer)
        
        # Update test result
        test_result.end_time = datetime.utcnow()
        test_result.total_score = total_score
        test_result.completed = True
        
        db.session.commit()
        
        flash("Test submitted successfully!", "success")
        return redirect(url_for('routes.test_results', result_id=result_id))


    def auto_submit_test(result_id):
        test_result = TestResult.query.get_or_404(result_id)
        
        # Mark test as completed
        test_result.end_time = datetime.utcnow()
        test_result.completed = True
        
        # Grade all answered questions
        total_score = 0
        for answer in test_result.answers:
            question = Question.query.get(answer.question_id)
            
            # Grade based on question type
            if question.question_type in [QuestionType.MULTIPLE_CHOICE, QuestionType.TRUE_FALSE]:
                # For multiple choice and true/false, exact match required
                is_correct = answer.student_answer == question.correct_answer
                score = question.marks if is_correct else 0
            elif question.question_type == QuestionType.NUMERICAL:
                # For numerical, allow a small margin of error
                try:
                    student_value = float(answer.student_answer.strip())
                    correct_value = float(question.correct_answer.strip())
                    is_correct = abs(student_value - correct_value) < 0.001
                    score = question.marks if is_correct else 0
                except (ValueError, TypeError):
                    is_correct = False
                    score = 0
            else:
                # For descriptive questions, can't auto-grade
                is_correct = None
                score = question.marks * 0.5 if answer.student_answer else 0
            
            answer.is_correct = is_correct
            answer.score = score
            total_score += score
        
        # Add empty answers for unanswered questions
        test = Test.query.get(test_result.test_id)
        test_questions = TestQuestion.query.filter_by(test_id=test.id).all()
        answered_question_ids = [a.question_id for a in test_result.answers]
        
        for tq in test_questions:
            if tq.question_id not in answered_question_ids:
                new_answer = QuestionAnswer(
                    test_result_id=test_result.id,
                    question_id=tq.question_id,
                    student_answer="",
                    is_correct=False,
                    score=0
                )
                db.session.add(new_answer)
        
        test_result.total_score = total_score
        db.session.commit()
        
        flash("Time's up! Your test has been automatically submitted.", "info")
        return redirect(url_for('routes.test_results', result_id=result_id))


    @routes_bp.route('/student/test_results/<int:result_id>')
    @login_required
    def test_results(result_id):
        if current_user.role != UserRole.STUDENT:
            flash("Access denied. Student permissions required.", "danger")
            return redirect(url_for('routes.index'))
        
        test_result = TestResult.query.get_or_404(result_id)
        
        # Ensure the student owns this test result
        if test_result.student_id != current_user.id:
            flash("Access denied. This is not your test.", "danger")
            return redirect(url_for('routes.student_dashboard'))
        
        test = Test.query.get(test_result.test_id)
        
        # Get all answers with questions
        answers = db.session.query(QuestionAnswer, Question).\
            join(Question, QuestionAnswer.question_id == Question.id).\
            filter(QuestionAnswer.test_result_id == result_id).all()
        
        # Get chapter performance
        chapter_performance = {}
        for answer, question in answers:
            chapter = Chapter.query.get(question.chapter_id)
            if chapter.name not in chapter_performance:
                chapter_performance[chapter.name] = {
                    'questions': 0,
                    'correct': 0,
                    'score': 0,
                    'max_score': 0
                }
            
            chapter_performance[chapter.name]['questions'] += 1
            chapter_performance[chapter.name]['max_score'] += question.marks
            
            if answer.score:
                chapter_performance[chapter.name]['score'] += answer.score
                
            if answer.is_correct:
                chapter_performance[chapter.name]['correct'] += 1
        
        # Calculate percentages
        for chapter, data in chapter_performance.items():
            if data['max_score'] > 0:
                data['percentage'] = round((data['score'] / data['max_score']) * 100, 1)
            else:
                data['percentage'] = 0
        
        # Calculate overall score percentage
        score_percentage = round((test_result.total_score / test.total_marks) * 100, 1) if test.total_marks > 0 else 0
        
        return render_template('student/test_results.html',
                              title='Test Results',
                              test=test,
                              test_result=test_result,
                              answers=answers,
                              chapter_performance=chapter_performance,
                              score_percentage=score_percentage)


    @routes_bp.route('/student/performance')
    @login_required
    def student_performance():
        if current_user.role != UserRole.STUDENT:
            flash("Access denied. Student permissions required.", "danger")
            return redirect(url_for('routes.index'))
        
        # Get all completed tests
        completed_tests = TestResult.query.filter_by(
            student_id=current_user.id,
            completed=True
        ).order_by(TestResult.end_time.asc()).all()
        
        # Get tests info with IST conversion
        test_data = []
        for result in completed_tests:
            test = Test.query.get(result.test_id)
            score_percentage = round((result.total_score / test.total_marks) * 100, 1) if test.total_marks > 0 else 0
            local_time = utc_to_local(result.end_time)
            
            test_data.append({
                'id': result.id,
                'test_id': test.id,
                'title': test.title,
                'date': local_time.strftime('%d %b, %I:%M %p IST'),
                'score': result.total_score,
                'total': test.total_marks,
                'percentage': score_percentage
            })
        
        # Get chapter performance data
        chapter_performance = {}
        
        for result in completed_tests:
            answers = db.session.query(QuestionAnswer, Question).\
                join(Question, QuestionAnswer.question_id == Question.id).\
                filter(QuestionAnswer.test_result_id == result.id).all()
                
            for answer, question in answers:
                chapter = Chapter.query.get(question.chapter_id)
                
                if chapter.name not in chapter_performance:
                    chapter_performance[chapter.name] = {
                        'questions': 0,
                        'correct': 0,
                        'score': 0,
                        'max_score': 0
                    }
                
                chapter_performance[chapter.name]['questions'] += 1
                chapter_performance[chapter.name]['max_score'] += question.marks
                
                if answer.score:
                    chapter_performance[chapter.name]['score'] += answer.score
                    
                if answer.is_correct:
                    chapter_performance[chapter.name]['correct'] += 1
        
        # Calculate percentages and identify strengths/weaknesses
        strengths = []
        weaknesses = []
        
        for chapter, data in chapter_performance.items():
            if data['max_score'] > 0:
                data['percentage'] = round((data['score'] / data['max_score']) * 100, 1)
                
                # Consider strong if > 75%, weak if < 50%
                if data['percentage'] >= 75:
                    strengths.append(chapter)
                elif data['percentage'] < 50:
                    weaknesses.append(chapter)
            else:
                data['percentage'] = 0
        
        # Calculate overall performance as average of percentages
        if completed_tests:
            percentages = [(result.total_score / Test.query.get(result.test_id).total_marks) for result in completed_tests]
            overall_percentage = round((sum(percentages) / len(percentages)) * 100, 1)
        else:
            overall_percentage = 0
        
        return render_template('student/performance.html',
                              title='My Performance',
                              test_data=test_data,
                              chapter_performance=chapter_performance,
                              strengths=strengths,
                              weaknesses=weaknesses,
                              overall_percentage=overall_percentage)

    return routes_bp
