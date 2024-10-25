from flask import Flask, request, jsonify
from flask_marshmallow import Marshmallow
from sql_connection import connect_database
from marshmallow import fields, ValidationError

app = Flask(__name__)
ma = Marshmallow(app)

class MemberSchema(ma.Schema):
    id = fields.Int(dump_only=True)
    name = fields.String(required=True)
    age = fields.String(required=True)
    
    class Meta:
        fields = ('id', 'name', 'age')

member_schema = MemberSchema()
members_schema = MemberSchema(many=True)

@app.route('/add_member', methods=['POST'])
def add_member():
    conn = connect_database()
    new_member = request.get_json()
    errors = member_schema.validate(new_member)

    if (errors):
        return jsonify(errors), 404
    else:
        cursor = conn.cursor()
        new_member_details = (new_member['id'], new_member['name'], new_member['age'])

        query = '''
            INSERT INTO Members (
                id,
                name,
                age
            ) VALUES (
                %s,
                %s,
                %s
            )
        '''
        
        cursor.execute(query, new_member_details)
            
        conn.commit()
        
        cursor.close()
        conn.close()
        
        return jsonify({'message': f'New member: {new_member["name"]} was added to the database!'}), 200

@app.route('/members')
def get_members():
    conn = connect_database()
    if (conn):
        cursor = conn.cursor(dictionary=True)
        
        query = '''
            SELECT *
            FROM Members;
        '''
        
        cursor.execute(query)
        members = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return members_schema.jsonify(members), 200

@app.route('/members/<int:id>', methods=['PUT'])
def update_member(id):
    try:
        member_info = member_schema.load(request.json, partial=True)
    except ValidationError as e:
        print(f'Validation Error: {e}')
        return jsonify(e.messages), 400
    
    try:
        conn = connect_database()
        if conn is None:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor()

        updated_member = (member_info['age'], member_info['name'], id)

        query = '''
            UPDATE Members
            SET age = %s,
                name = %s
            WHERE id = %s
        '''

        cursor.execute(query, updated_member)
        conn.commit()

        return jsonify({'message': 'Member updated successfully'}), 200

    except Exception as e:
        print(f'Error: {e}')
        return jsonify({'error': 'Internal Server Error'}), 500

    finally:
        if conn:
            cursor.close()
            conn.close()

@app.route('/members/<int:member_id>', methods=['DELETE'])
def delete_member(id):
    conn = connect_database()
    if (conn):
        cursor = conn.cursor()

        query = f'''
            SELECT *
            FROM Members
            WHERE id={id}
        '''
        cursor.execute(query)
        
        member = cursor.fetchone()
        
        if (member):
            query = f'''
                DELETE FROM Members
                WHERE id={id}
            '''
            
            cursor.execute(query)
                
            conn.commit()
            
            cursor.close()
            conn.close()
            
            return jsonify({'message': f'Member id: {id} has been deleted!'}), 200
        else:
            return jsonify({'message': 'Member not found!'})
        

class SessionSchema(ma.Schema):
    session_id = fields.Int(dump_only=True)
    member_id = fields.Int(dump_only=True)
    session_date = fields.Int(dump_only=True)
    session_time = fields.String(required=True)
    activity = fields.String(required=True)
    duration_minutes = fields.Int(dump_only=True)
    calories_burned = fields.Int(dump_only=True)
    
    class Meta:
        fields = ('session_id', 'member_id', 'session_date', 'session_time', 'activity', 'duration_minutes', 'calories_burned')

session_schema = SessionSchema()
sessions_schema = SessionSchema(many=True)


@app.route('/session/<int:session_id>', methods=['GET'])
def get_session(session_id):
    conn = connect_database()
    if (conn):
        cursor = conn.cursor(dictionary=True)

        query = f'''
            SELECT *
            FROM WorkoutSessions
            WHERE id={session_id}
        '''
        cursor.execute(query)
        
        session = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if (session):
            return session_schema.jsonify(session)        
        else:
            return jsonify({'message': 'Session not found!'})


@app.route('/sessions/<int:session_id>', methods=['DELETE'])
def delete_session(session_id):
    conn = connect_database()
    if (conn):
        cursor = conn.cursor()

        query = f'''
            SELECT *
            FROM WorkoutSessions
            WHERE id={session_id}
        '''
        cursor.execute(query)
        
        session = cursor.fetchone()
        
        if (session):
            query = f'''
                DELETE FROM WorkoutSessions
                WHERE id={session_id}
            '''
            
            cursor.execute(query)
                
            conn.commit()
            
            cursor.close()
            conn.close()
            
            return jsonify({'message': f'Session id: {session_id} has been deleted!'}), 200
        else:
            return jsonify({'message': 'Session not found!'})
        
@app.route('/sessions/<int:session_id>', methods=['PUT'])
def update_session(session_id):
    conn = connect_database()
    updated_session = request.get_json()
    errors = session_schema.validate(updated_session)

    if (errors):
        return jsonify(errors), 404
    else:
        cursor = conn.cursor()

        query = f'''
            SELECT *
            FROM WorkoutSessions
            WHERE id={session_id}
        '''
        cursor.execute(query)
        
        session = cursor.fetchone()
        
        if (session):
            updated_session_details = (updated_session['session_id'], updated_session['member_id'], updated_session['session_date'], updated_session['session_time'], updated_session['activity'], updated_session['duration_minutes'], updated_session['calories_burned'] )
            
            query = f'''
                UPDATE WorkoutSessions
                SET
                    session_id = %s,
                    member_id = %s,
                    session_date = %s,
                    session_time = %s,
                    activity = %s,
                    duration_minutes = %s,
                    calories_burned = %s
                WHERE id={session_id}
            '''
            cursor.execute(query, updated_session_details)
                
            conn.commit()

            cursor.close()
            conn.close()
            
            return jsonify({'message': f'{updated_session["name"]} has been updated!'}), 200
        else:
            return jsonify({'message': 'Session not found!'})

@app.route('/add_session', methods=['POST'])
def add_session():
    conn = connect_database()
    new_session = request.get_json()
    errors = session_schema.validate(new_session)

    if (errors):
        return jsonify(errors), 404
    else:
        cursor = conn.cursor()
        new_session_details = (new_session['session_id'], new_session['member_id'], new_session['session_date'], new_session['session_time'], new_session['activity'], new_session['duration_minutes'], new_session['calories_burned'])

        query = '''
            INSERT INTO WorkoutSessions (
                session_id,
                member_id,
                session_date,
                session_time,
                activity,
                duration_minutes,
                calories_burned
            ) VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )
        '''
        
        cursor.execute(query, new_session_details)
            
        conn.commit()
        
        cursor.close()
        conn.close()
        
        return jsonify({'message': f'New session: {new_session["session_id"]} was added to the database!'}), 200            