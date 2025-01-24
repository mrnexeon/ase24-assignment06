package de.unibayreuth.se.taskboard.data.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.unibayreuth.se.taskboard.business.domain.Task;
import de.unibayreuth.se.taskboard.business.domain.TaskStatus;
import de.unibayreuth.se.taskboard.business.exceptions.TaskNotFoundException;
import de.unibayreuth.se.taskboard.business.ports.TaskPersistenceService;
import de.unibayreuth.se.taskboard.data.mapper.TaskEntityMapper;
import de.unibayreuth.se.taskboard.data.persistence.EventEntity;
import de.unibayreuth.se.taskboard.data.persistence.EventRepository;
import de.unibayreuth.se.taskboard.data.persistence.TaskRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Primary;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Event-sourcing-based implementation of the task persistence service that the business layer provides as a port.
 */
@Service
@RequiredArgsConstructor
@Primary
public class TaskPersistenceServiceEventSourcingImpl implements TaskPersistenceService {
    private final TaskRepository taskRepository;
    private final TaskEntityMapper taskEntityMapper;
    private final EventRepository eventRepository;
    private final ObjectMapper objectMapper;
    @Override
    public void clear() {
        taskRepository.findAll()
                .forEach(taskEntity -> eventRepository.saveAndFlush(
                        EventEntity.deleteEventOf(taskEntityMapper.fromEntity(taskEntity), null))
                );
        if (taskRepository.count() != 0) {
            throw new IllegalStateException("Tasks not successfully deleted.");
        }
    }

    @NonNull
    @Override
    public List<Task> getAll() {
        return taskRepository.findAll().stream()
                .map(taskEntityMapper::fromEntity)
                .toList();
    }

    @NonNull
    @Override
    public Optional<Task> getById(@NonNull UUID id) {
        return taskRepository.findById(id)
                .map(taskEntityMapper::fromEntity);
    }

    @NonNull
    @Override
    public List<Task> getByStatus(@NonNull TaskStatus status) {
        return taskRepository.findByStatus(status).stream()
                .map(taskEntityMapper::fromEntity)
                .toList();
    }

    @NonNull
    @Override
    public List<Task> getByAssignee(@NonNull UUID userId) {
        return taskRepository.findByAssigneeId(userId).stream()
                .map(taskEntityMapper::fromEntity)
                .toList();
    }

    @NonNull
    @Override
    public Task upsert(@NonNull Task task) throws TaskNotFoundException {
        if (task.getId() == null) {
            task.setId(UUID.randomUUID());
            eventRepository.saveAndFlush(EventEntity.insertEventOf(task, task.getAssigneeId(), objectMapper));
            taskRepository.save(taskEntityMapper.toEntity(task));
            return task;
        } else {
            Optional<Task> existingTask = taskRepository.findById(task.getId()).map(taskEntityMapper::fromEntity);
            if (existingTask.isPresent()) {
                eventRepository.saveAndFlush(EventEntity.updateEventOf(task, task.getAssigneeId(), objectMapper));
                taskRepository.save(taskEntityMapper.toEntity(task));
                return task;
            } else {
                throw new TaskNotFoundException("Task with ID " + task.getId() + " does not exist.");
            }
        }
    }

    @Override
    public void delete(@NonNull UUID id) throws TaskNotFoundException {
        Optional<Task> task = taskRepository.findById(id).map(taskEntityMapper::fromEntity);
        if (task.isPresent()) {
            eventRepository.saveAndFlush(EventEntity.deleteEventOf(task.get(), task.get().getAssigneeId()));
            taskRepository.deleteById(id);
            Optional<Task> removedTask = taskRepository.findById(id).map(taskEntityMapper::fromEntity);
            if (removedTask.isPresent()) {
                throw new IllegalStateException("Task delete with " + id + " was not successfully.");
            } else {
            }
        } else {
            throw new TaskNotFoundException("Task with ID " + id + " does not exist.");
        }
    }
}
