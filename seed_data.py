from app import db, create_app
from models import User, Chapter, Question, QuestionDifficulty, QuestionType, UserRole
import sys

def seed_database():
    """Seed the database with initial data if tables are empty"""
    try:
        app = create_app()
        with app.app_context():
            print("Starting database seeding...")
            # Check if we need to seed
            if User.query.count() > 0:
                return  # Database already has data
            
            print("Seeding database with initial data...")
            
            # Create chapters
            chapters = [
                {"name": "Light - Reflection and Refraction", "description": "Principles of reflection and refraction of light"},
                {"name": "Electricity", "description": "Electric current, potential difference, and circuits"},
                {"name": "Magnetic Effects of Electric Current", "description": "Magnetic fields, electromagnetic induction"},
                {"name": "Sources of Energy", "description": "Conventional and non-conventional sources of energy"},
                {"name": "Human Eye and Colorful World", "description": "Human eye, defects of vision, and optical phenomena"}
            ]
            
            chapter_objects = []
            for chapter_data in chapters:
                chapter = Chapter(**chapter_data)
                db.session.add(chapter)
                chapter_objects.append(chapter)
            
            # Commit to get chapter IDs
            db.session.commit()
            
            # Create sample questions
            questions = [
                # Light - Reflection and Refraction
                {
                    "text": "The image formed by a plane mirror is:",
                    "chapter_id": 1,
                    "difficulty": QuestionDifficulty.EASY,
                    "question_type": QuestionType.MULTIPLE_CHOICE,
                    "marks": 1,
                    "option_a": "Real and inverted",
                    "option_b": "Virtual and inverted",
                    "option_c": "Real and erect",
                    "option_d": "Virtual and erect",
                    "correct_answer": "D",
                    "solution": "A plane mirror forms a virtual and erect image. The image is formed behind the mirror at the same distance as the object is in front of the mirror."
                },
                {
                    "text": "The refractive index of glass with respect to air is 1.5. What is the refractive index of air with respect to glass?",
                    "chapter_id": 1,
                    "difficulty": QuestionDifficulty.MEDIUM,
                    "question_type": QuestionType.NUMERICAL,
                    "marks": 2,
                    "correct_answer": "0.67",
                    "solution": "If the refractive index of glass with respect to air is n_ga = 1.5, then the refractive index of air with respect to glass is n_ag = 1/n_ga = 1/1.5 = 0.67."
                },
                {
                    "text": "Explain the phenomenon of total internal reflection with examples.",
                    "chapter_id": 1,
                    "difficulty": QuestionDifficulty.HARD,
                    "question_type": QuestionType.DESCRIPTIVE,
                    "marks": 3,
                    "correct_answer": "Total internal reflection occurs when light travels from a denser medium to a rarer medium and the angle of incidence exceeds the critical angle.",
                    "solution": "Total internal reflection occurs when light travels from a denser medium to a rarer medium, and the angle of incidence exceeds the critical angle. At this point, all light is reflected back into the denser medium. Examples include the working of optical fibers, mirage formation, and the sparkling of diamonds."
                },
                
                # Electricity
                {
                    "text": "Which of the following is the SI unit of electric current?",
                    "chapter_id": 2,
                    "difficulty": QuestionDifficulty.EASY,
                    "question_type": QuestionType.MULTIPLE_CHOICE,
                    "marks": 1,
                    "option_a": "Volt",
                    "option_b": "Ampere",
                    "option_c": "Ohm",
                    "option_d": "Watt",
                    "correct_answer": "B",
                    "solution": "The SI unit of electric current is the Ampere (A)."
                },
                {
                    "text": "Ohm's law states that the current through a conductor is proportional to the voltage across it.",
                    "chapter_id": 2,
                    "difficulty": QuestionDifficulty.EASY,
                    "question_type": QuestionType.TRUE_FALSE,
                    "marks": 1,
                    "correct_answer": "True",
                    "solution": "Ohm's law states that the current through a conductor between two points is directly proportional to the voltage across the two points, provided the temperature and other physical conditions remain constant."
                },
                {
                    "text": "Calculate the resistance of a circuit if a current of 2A flows when a voltage of 12V is applied.",
                    "chapter_id": 2,
                    "difficulty": QuestionDifficulty.MEDIUM,
                    "question_type": QuestionType.NUMERICAL,
                    "marks": 2,
                    "correct_answer": "6",
                    "solution": "Using Ohm's law, R = V/I = 12V/2A = 6 ohms."
                },
                
                # Magnetic Effects of Electric Current
                {
                    "text": "The direction of magnetic field lines around a straight current-carrying conductor is given by:",
                    "chapter_id": 3,
                    "difficulty": QuestionDifficulty.MEDIUM,
                    "question_type": QuestionType.MULTIPLE_CHOICE,
                    "marks": 2,
                    "option_a": "Left-hand thumb rule",
                    "option_b": "Right-hand thumb rule",
                    "option_c": "Fleming's left-hand rule",
                    "option_d": "Fleming's right-hand rule",
                    "correct_answer": "B",
                    "solution": "The right-hand thumb rule gives the direction of the magnetic field around a straight current-carrying conductor. If you grasp the conductor with your right hand such that the thumb points in the direction of the current, then the fingers wrapped around the conductor give the direction of the magnetic field."
                },
                {
                    "text": "Explain the working principle of an electric motor.",
                    "chapter_id": 3,
                    "difficulty": QuestionDifficulty.HARD,
                    "question_type": QuestionType.DESCRIPTIVE,
                    "marks": 3,
                    "correct_answer": "An electric motor works on the principle of magnetic force experienced by a current-carrying conductor in a magnetic field.",
                    "solution": "An electric motor works on the principle of magnetic force experienced by a current-carrying conductor in a magnetic field. When a current-carrying coil is placed in a magnetic field, it experiences a torque that rotates the coil. The direction of rotation is given by Fleming's left-hand rule. The split-ring commutator reverses the direction of current every half rotation, ensuring continuous rotation in the same direction."
                },
                
                # Sources of Energy
                {
                    "text": "Which of the following is a non-renewable source of energy?",
                    "chapter_id": 4,
                    "difficulty": QuestionDifficulty.EASY,
                    "question_type": QuestionType.MULTIPLE_CHOICE,
                    "marks": 1,
                    "option_a": "Solar energy",
                    "option_b": "Wind energy",
                    "option_c": "Coal",
                    "option_d": "Hydro energy",
                    "correct_answer": "C",
                    "solution": "Coal is a non-renewable source of energy as it takes millions of years to form and cannot be replenished in a human lifetime."
                },
                {
                    "text": "Solar cells convert solar energy directly into electrical energy.",
                    "chapter_id": 4,
                    "difficulty": QuestionDifficulty.EASY,
                    "question_type": QuestionType.TRUE_FALSE,
                    "marks": 1,
                    "correct_answer": "True",
                    "solution": "Solar cells, or photovoltaic cells, directly convert solar energy (light) into electrical energy through the photovoltaic effect."
                },
                
                # Human Eye and Colorful World
                {
                    "text": "The defect of vision in which a person cannot see nearby objects clearly is:",
                    "chapter_id": 5,
                    "difficulty": QuestionDifficulty.MEDIUM,
                    "question_type": QuestionType.MULTIPLE_CHOICE,
                    "marks": 2,
                    "option_a": "Myopia",
                    "option_b": "Hypermetropia",
                    "option_c": "Presbyopia",
                    "option_d": "Astigmatism",
                    "correct_answer": "B",
                    "solution": "Hypermetropia, or farsightedness, is a defect of vision in which a person cannot see nearby objects clearly but can see distant objects clearly. This occurs when the eyeball is too short or the eye lens is unable to become thick enough."
                },
                {
                    "text": "What is the cause of dispersion of white light through a prism?",
                    "chapter_id": 5,
                    "difficulty": QuestionDifficulty.HARD,
                    "question_type": QuestionType.DESCRIPTIVE,
                    "marks": 3,
                    "correct_answer": "The dispersion of white light through a prism is caused by the fact that different colors of light have different wavelengths and, therefore, different angles of refraction.",
                    "solution": "The dispersion of white light through a prism is caused by the fact that white light is composed of seven colors (VIBGYOR), each with a different wavelength. Different wavelengths of light travel at different speeds in a medium, resulting in different angles of refraction. Violet light has the shortest wavelength and is refracted the most, while red light has the longest wavelength and is refracted the least."
                }
            ]
            
            for question_data in questions:
                question = Question(**question_data)
                db.session.add(question)
            
            # Create default teacher and student
            teacher = User(
                username="teacher",
                email="teacher@example.com",
                role=UserRole.TEACHER
            )
            teacher.set_password("password")
            db.session.add(teacher)
            
            student = User(
                username="student",
                email="student@example.com",
                role=UserRole.STUDENT,
                teacher_id=1  # Will be linked to the teacher above
            )
            student.set_password("password")
            db.session.add(student)
            
            db.session.commit()
            print("Database seeded successfully!")
    except Exception as e:
        print(f"Error seeding database: {str(e)}", file=sys.stderr)
        print("Traceback:", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    seed_database()
