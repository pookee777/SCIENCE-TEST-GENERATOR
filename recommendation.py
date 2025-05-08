"""
Machine learning system for personalized question recommendations
"""

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors
from sklearn.decomposition import PCA

from models import User, Question, QuestionAnswer, TestResult, Chapter, QuestionDifficulty, QuestionType
from app import db


class RecommendationEngine:
    """
    Machine learning recommendation engine for personalized question suggestions
    based on a student's performance history.
    """
    
    def __init__(self):
        self.scaler = StandardScaler()
        self.pca = PCA(n_components=5)  # Reduce to 5 dimensions for efficiency
        self.model = NearestNeighbors(n_neighbors=10, algorithm='ball_tree')
        self.questions_df = None
        self.questions_features = None
        self.student_profiles = {}
    
    def get_questions_dataframe(self):
        """
        Create a DataFrame of all questions with relevant features
        """
        # Query all questions
        questions = Question.query.all()
        
        # Create DataFrame
        data = []
        for q in questions:
            # Calculate question popularity (how often it's answered correctly)
            answers = QuestionAnswer.query.filter_by(question_id=q.id).all()
            correct_count = sum(1 for a in answers if a.is_correct)
            total_count = len(answers)
            correctness_rate = correct_count / total_count if total_count > 0 else 0.5
            
            # Encode difficulty
            difficulty_map = {
                QuestionDifficulty.EASY: 1,
                QuestionDifficulty.MEDIUM: 2,
                QuestionDifficulty.HARD: 3
            }
            
            # Encode question type
            type_map = {
                QuestionType.MULTIPLE_CHOICE: 1,
                QuestionType.TRUE_FALSE: 2,
                QuestionType.NUMERICAL: 3,
                QuestionType.DESCRIPTIVE: 4
            }
            
            data.append({
                'question_id': q.id,
                'chapter_id': q.chapter_id,
                'difficulty': difficulty_map.get(q.difficulty, 2),
                'question_type': type_map.get(q.question_type, 1),
                'marks': q.marks,
                'popularity': correctness_rate,
                'text_length': len(q.text) / 500  # Normalized by 500 chars
            })
            
        return pd.DataFrame(data)
    
    def create_student_profile(self, student_id):
        """
        Create a profile vector for a student based on their performance
        """
        # Get student's answered questions
        answers = db.session.query(QuestionAnswer, Question)\
            .join(TestResult, QuestionAnswer.test_result_id == TestResult.id)\
            .join(Question, QuestionAnswer.question_id == Question.id)\
            .filter(TestResult.student_id == student_id)\
            .filter(TestResult.completed == True)\
            .all()
        
        if not answers:
            return None
            
        # Create student profile based on question characteristics they perform well on
        chapter_scores = {}
        difficulty_scores = {1: [], 2: [], 3: []}  # Easy, Medium, Hard
        type_scores = {1: [], 2: [], 3: [], 4: []}  # MC, TF, Numerical, Descriptive
        
        for answer, question in answers:
            # Skip if no score recorded
            if answer.score is None:
                continue
                
            # Calculate score percentage for this question
            score_pct = answer.score / question.marks if question.marks > 0 else 0
            
            # Record chapter performance
            if question.chapter_id not in chapter_scores:
                chapter_scores[question.chapter_id] = []
            chapter_scores[question.chapter_id].append(score_pct)
            
            # Record difficulty performance
            difficulty_val = 1  # Default to medium
            if question.difficulty == QuestionDifficulty.EASY:
                difficulty_val = 1
            elif question.difficulty == QuestionDifficulty.MEDIUM:
                difficulty_val = 2
            elif question.difficulty == QuestionDifficulty.HARD:
                difficulty_val = 3
            difficulty_scores[difficulty_val].append(score_pct)
            
            # Record question type performance
            type_val = 1  # Default to multiple choice
            if question.question_type == QuestionType.MULTIPLE_CHOICE:
                type_val = 1
            elif question.question_type == QuestionType.TRUE_FALSE:
                type_val = 2
            elif question.question_type == QuestionType.NUMERICAL:
                type_val = 3
            elif question.question_type == QuestionType.DESCRIPTIVE:
                type_val = 4
            type_scores[type_val].append(score_pct)
        
        # Calculate average performance by chapter
        chapter_avg = {}
        for chapter_id, scores in chapter_scores.items():
            if scores:
                chapter_avg[chapter_id] = sum(scores) / len(scores)
            else:
                chapter_avg[chapter_id] = 0.5  # Default for no data
        
        # Find weak chapters (below 65% average score)
        weak_chapters = [chapter_id for chapter_id, avg in chapter_avg.items() if avg < 0.65]
        
        # Calculate performance by difficulty
        difficulty_avg = {}
        for diff, scores in difficulty_scores.items():
            if scores:
                difficulty_avg[diff] = sum(scores) / len(scores)
            else:
                difficulty_avg[diff] = 0.5  # Default
        
        # Calculate performance by question type
        type_avg = {}
        for qtype, scores in type_scores.items():
            if scores:
                type_avg[qtype] = sum(scores) / len(scores)
            else:
                type_avg[qtype] = 0.5  # Default
        
        # Create student preference vector
        # Lower scores mean student needs more practice (higher recommendation weight)
        profile = {
            'weak_chapters': weak_chapters,
            'difficulty_preference': {
                d: 1 - score for d, score in difficulty_avg.items()  # Invert scores
            },
            'type_preference': {
                t: 1 - score for t, score in type_avg.items()  # Invert scores
            },
            'chapter_preference': {
                c: 1 - score for c, score in chapter_avg.items()  # Invert scores
            }
        }
        
        return profile
    
    def train_model(self):
        """
        Train the recommendation model based on question features
        """
        # Get questions data
        self.questions_df = self.get_questions_dataframe()
        
        if len(self.questions_df) == 0:
            return False
            
        # Extract features for model training
        features = self.questions_df[['chapter_id', 'difficulty', 'question_type', 
                                     'marks', 'popularity', 'text_length']]
        
        # Normalize features
        features_scaled = self.scaler.fit_transform(features)
        
        # Dimensionality reduction
        if len(features_scaled) > 5:  # Only use PCA if we have enough samples
            features_pca = self.pca.fit_transform(features_scaled)
            self.model.fit(features_pca)
            self.questions_features = features_pca
        else:
            self.model.fit(features_scaled)
            self.questions_features = features_scaled
            
        return True
    
    def recommend_questions(self, student_id, chapter_ids=None, num_questions=10):
        """
        Recommend questions for a student based on their profile
        
        Args:
            student_id: The ID of the student
            chapter_ids: Optional list of chapter IDs to filter by
            num_questions: Number of questions to recommend
            
        Returns:
            List of recommended Question objects
        """
        # Ensure model is trained
        if self.questions_df is None:
            self.train_model()
        
        # Get or create student profile
        if student_id not in self.student_profiles:
            self.student_profiles[student_id] = self.create_student_profile(student_id)
            
        student_profile = self.student_profiles[student_id]
        
        # If no profile (new student), recommend a mix of questions
        if student_profile is None:
            # Get random questions, possibly filtered by chapter
            query = Question.query
            if chapter_ids:
                query = query.filter(Question.chapter_id.in_(chapter_ids))
                
            # Get a mix of difficulties
            easy = query.filter_by(difficulty=QuestionDifficulty.EASY).limit(num_questions // 3).all()
            medium = query.filter_by(difficulty=QuestionDifficulty.MEDIUM).limit(num_questions // 3).all()
            hard = query.filter_by(difficulty=QuestionDifficulty.HARD).limit(num_questions // 3).all()
            
            recommendations = easy + medium + hard
            
            # If we don't have enough, add more random questions
            if len(recommendations) < num_questions:
                additional = query.limit(num_questions - len(recommendations)).all()
                recommendations.extend(additional)
                
            # Shuffle and return
            np.random.shuffle(recommendations)
            return recommendations[:num_questions]
        
        # Filter by chapters if provided
        filtered_df = self.questions_df
        if chapter_ids:
            filtered_df = filtered_df[filtered_df['chapter_id'].isin(chapter_ids)]
            
        # If no questions match the filter, return empty list
        if len(filtered_df) == 0:
            return []
            
        # Apply student profile to weight questions
        scores = []
        for idx, row in filtered_df.iterrows():
            score = 0
            
            # Increase score for weak chapters
            if row['chapter_id'] in student_profile['weak_chapters']:
                score += 5
            
            # Add chapter preference score (0-1)
            chapter_pref = student_profile['chapter_preference'].get(row['chapter_id'], 0.5)
            score += chapter_pref * 3
            
            # Add difficulty preference score (0-1)
            difficulty_pref = student_profile['difficulty_preference'].get(int(row['difficulty']), 0.5)
            score += difficulty_pref * 2
            
            # Add question type preference score (0-1)
            type_pref = student_profile['type_preference'].get(int(row['question_type']), 0.5)
            score += type_pref * 2
            
            scores.append(score)
        
        # Normalize scores
        max_score = max(scores) if scores else 1
        norm_scores = [s/max_score for s in scores]
        
        # Apply weighted random selection based on scores
        probabilities = np.array(norm_scores) / sum(norm_scores)
        
        # Get indices of recommended questions
        if len(filtered_df) <= num_questions:
            # If we have fewer questions than requested, return all of them
            chosen_indices = list(range(len(filtered_df)))
        else:
            # Otherwise use weighted random selection
            chosen_indices = np.random.choice(
                len(filtered_df), 
                size=min(num_questions, len(filtered_df)), 
                replace=False, 
                p=probabilities
            )
        
        # Get corresponding question IDs
        recommended_ids = filtered_df.iloc[chosen_indices]['question_id'].tolist()
        
        # Fetch and return question objects
        recommended_questions = Question.query.filter(Question.id.in_(recommended_ids)).all()
        
        return recommended_questions


# Create singleton instance
recommender = RecommendationEngine()