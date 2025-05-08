import os
import sys
import shutil
from werkzeug.utils import secure_filename

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app
from models import Question, QuestionDifficulty, QuestionType
from extensions import db

ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}
UPLOAD_FOLDER = 'static/question_images'

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def add_question_interactive():
    app = create_app()
    with app.app_context():
        try:
            # Get question details from user
            text = input("Enter question text: ")
            chapter_id = int(input("Enter chapter ID: "))
            print("\nDifficulty levels:")
            print("1. Easy\n2. Medium\n3. Hard")
            diff_choice = int(input("Select difficulty (1-3): "))
            difficulty = {1: QuestionDifficulty.EASY, 2: QuestionDifficulty.MEDIUM, 3: QuestionDifficulty.HARD}[diff_choice]
            
            print("\nQuestion types:")
            print("1. Multiple Choice\n2. True/False\n3. Numerical\n4. Descriptive")
            type_choice = int(input("Select type (1-4): "))
            q_type = {
                1: QuestionType.MULTIPLE_CHOICE,
                2: QuestionType.TRUE_FALSE,
                3: QuestionType.NUMERICAL,
                4: QuestionType.DESCRIPTIVE
            }[type_choice]
            
            marks = int(input("Enter marks: "))
            
            # Get type-specific details
            option_a = option_b = option_c = option_d = None
            if q_type == QuestionType.MULTIPLE_CHOICE:
                option_a = input("Enter option A: ")
                option_b = input("Enter option B: ")
                option_c = input("Enter option C: ")
                option_d = input("Enter option D: ")
                print("\nFor multiple choice, enter the correct option letter (A, B, C, or D)")
                correct_answer = input("Enter correct option: ").upper()
                while correct_answer not in ['A', 'B', 'C', 'D']:
                    print("Invalid option! Please enter A, B, C, or D")
                    correct_answer = input("Enter correct option: ").upper()
            elif q_type == QuestionType.TRUE_FALSE:
                print("\nEnter True or False for the correct answer")
                correct_answer = input("Enter correct answer: ").capitalize()
                while correct_answer not in ['True', 'False']:
                    print("Invalid answer! Please enter True or False")
                    correct_answer = input("Enter correct answer: ").capitalize()
            else:
                correct_answer = input("Enter correct answer: ")
            
            solution = input("Enter solution (optional): ")

            # Handle image upload
            has_image = input("\nDo you want to add an image? (y/n): ").lower() == 'y'
            image_path = None
            
            if has_image:
                while True:
                    image_file = input("Enter the full path to your image file: ")
                    if os.path.exists(image_file) and allowed_file(image_file):
                        # Create upload folder if it doesn't exist
                        os.makedirs(UPLOAD_FOLDER, exist_ok=True)
                        
                        # Generate secure filename and copy file
                        filename = secure_filename(os.path.basename(image_file))
                        dest_path = os.path.join(UPLOAD_FOLDER, filename)
                        shutil.copy2(image_file, dest_path)
                        
                        # Store relative path in database
                        image_path = os.path.join('question_images', filename)
                        print(f"Image uploaded successfully!")
                        break
                    else:
                        print("Invalid file or unsupported format. Please use .png, .jpg, .jpeg, or .gif")

            # Create and add question
            new_question = Question(
                text=text,
                chapter_id=chapter_id,
                difficulty=difficulty,
                question_type=q_type,
                marks=marks,
                option_a=option_a,
                option_b=option_b,
                option_c=option_c,
                option_d=option_d,
                correct_answer=correct_answer,
                solution=solution,
                image_path=image_path
            )
            
            db.session.add(new_question)
            db.session.commit()
            print("\nQuestion added successfully!")
            
        except Exception as e:
            print(f"Error adding question: {e}")
            db.session.rollback()

if __name__ == "__main__":
    add_question_interactive()
